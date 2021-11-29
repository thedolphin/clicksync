#!/usr/bin/python3

import sys
import itertools
import datetime
import multiprocessing
import logging

import yaml
import clickhouse_driver


class Timing:

    def __init__(self, logger, context_name):
        self.context_name = context_name
        self.start = datetime.datetime.min
        self.log = logger


    def __enter__(self):
        self.start = datetime.datetime.now()
        self.log.info('begin %s', self.context_name)
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        self.log.info('finish %s: %s',
            self.context_name,
            datetime.datetime.now() - self.start)


def get_tables(db, dbname):

    res = db.execute("select name from system.tables "
                     "where database = %(database)s and engine = 'MergeTree'",
                     {'database': dbname})

    return [v[0] for v in res]


def get_partitions(db, cname, dbname, tablename, days = None):

    if days:
        res = db.execute("select partition, sum(rows) as rows "
                         "from cluster(%(cluster)s, system.parts) "
                         "where database = %(database)s and `table` = %(table)s "
                         "and active and modification_time > %(mtime)s group by partition",
                         {'cluster': cname, 'database': dbname, 'table': tablename,
                         'mtime': datetime.datetime.now() - datetime.timedelta(days)})

    else:
        res = db.execute("select partition, sum(rows) as rows "
                         "from cluster(%(cluster)s, system.parts) "
                         "where database = %(database)s and `table` = %(table)s "
                         "and active group by partition",
                         {'cluster': cname, 'database': dbname, 'table': tablename})

    return {row[0]: row[1] for row in res}


def get_partition_key(db, dbname, tablename):

    res = db.execute("select partition_key from system.tables "
                      "where database = %(database)s and name = %(name)s",
                      {'database': dbname, 'name': tablename})

    return res[0][0]


def get_table_columns(db, dbname, tablename):

    res = db.execute("select name from system.columns "
                     "where database = %(database)s and `table` = %(table)s",
                     {'database': dbname, 'table': tablename})

    return ', '.join([v[0] for v in res])


def get_create_table_query(db, dbname, tablename):

    res = db.execute("select create_table_query from system.tables "
                     "where database = %(database)s and name = %(table)s",
                     {'database': dbname, 'table': tablename})

    return res[0][0] if res else None


def database_exists(db, dbname):

    res = db.execute("select name from system.databases where name = %(database)s",
                     {'database': dbname})

    return bool(res)


def copy_partition(db_src_host, db_dst_host, dbname, tablename, partition):

    # TODO: table is not partitioned
    if partition == 'tuple()':
        return True

    db_src = clickhouse_driver.Client(
        host=db_src_host,
        user=settings['clusters']['src']['user'],
        password=settings['clusters']['src']['pass'])

    part_key = get_partition_key(db_src, dbname, tablename)

    # quote partition name if type of partition key is string or datetime
    try:
        part_sample = db_src.execute(f"select {part_key} from {dbname}.{tablename} limit 1")[0][0]
        if isinstance(part_sample, (str, datetime.datetime)):
            partition = f"'{partition}'"
    except IndexError:
        log.info("table %s.%s is empty, won't copy", dbname, tablename)
        return True

    db_dst = clickhouse_driver.Client(
        host=db_dst_host,
        user=settings['clusters']['dst']['user'],
        password=settings['clusters']['dst']['pass'])

    columns = get_table_columns(db_src, dbname, tablename)

    with Timing(log, f"copying {dbname}.{tablename}[{partition}] {db_src_host} -> {db_dst_host}") as t:

        query = ""

        try:

            if settings.get('direct'):

                query = f"insert into {dbname}.{tablename} ({columns}) " \
                        f"select {columns} from remote('{db_src_host}', {dbname}.{tablename}, " \
                        f"'{settings['clusters']['src']['user']}', '{settings['clusters']['src']['pass']}') " \
                        f"where {part_key} = {partition}"

                log.debug(query)
                db_dst.execute(query)

            else:

                res = db_dst.execute(f'insert into {dbname}.{tablename} ({columns}) values',
                    db_src.execute(f'select {columns} from {dbname}.{tablename} '
                                   f'where {part_key} = {partition}'))

                t.context_name += f' ({res} rows copied)'

            return True

        except Exception as err:

            log.info(err, exc_info=True)
            if query:
                log.info('failed query was: %s', query)

            return False

        finally:
            db_src.disconnect()
            db_dst.disconnect()


def main():

    dbs = {'src': [], 'dst': []}

    for target in settings['clusters']:

        log.info('getting %s cluster configuration', target)

        with clickhouse_driver.Client(
            host=settings['clusters'][target]['host'],
            user=settings['clusters'][target]['user'],
            password=settings['clusters'][target]['pass']) as db:

            for row in db.execute('select host_address, host_name from system.clusters '
                                  'where cluster = %(cluster)s',
                                  {'cluster': settings['clusters'][target]['name']}):

                log.info('%s %s %s(%s)', target,
                         settings['clusters'][target]['name'],
                         row[1], row[0])

                dbs[target].append(clickhouse_driver.Client(
                    host=row[0],
                    user=settings['clusters'][target]['user'],
                    password=settings['clusters'][target]['pass']))

    for dbname in settings['databases']:

        log.info("checking database: %s", dbname)
        for db in dbs['dst']:
            if not database_exists(db, dbname):

                log.info("database %s does not exist on %s, will create",
                         dbname, db.connection.hosts[0][0])

                db.execute(f"create database {dbname}")


        dst_dbs = itertools.cycle(dbs['dst'])

        for table in get_tables(dbs['src'][0], dbname):

            log.info("checking table structure for table: %s", table)

            src_ctq = get_create_table_query(dbs['src'][0], dbname, table)

            for db in dbs['dst']:

                dst_ctq = get_create_table_query(db, dbname, table)

                if dst_ctq:
                    if dst_ctq != src_ctq:

                        log.info("table %s.%s has different structure on %s; will drop and refill",
                                 dbname, table, db.connection.hosts[0][0])

                        db.execute(f"drop table {dbname}.{table}")
                        db.execute(src_ctq)
                else:

                    log.info("table %s.%s does not exist on %s, will create",
                             dbname, table, db.connection.hosts[0][0])

                    db.execute(src_ctq)

            args = []

            log.info("checking partitions for table: %s", table)

            src_parts = get_partitions(
                dbs['src'][0],
                settings['clusters']['src']['name'],
                dbname, table,
                settings.get('days'))

            dst_parts = get_partitions(
                dbs['dst'][0],
                settings['clusters']['dst']['name'],
                dbname, table)

            for part in src_parts.keys():

                log.info("checking partition %s['%s']", table, part)
                dst_part = dst_parts.get(part)

                if src_parts[part] == dst_part:
                    log.info("partition %s['%s']: OK, %s rows", table, part, dst_part)

                else:

                    if dst_part:
                        log.info("partition %s['%s']: MISMATCH, %s => %s rows",
                                 table, part, src_parts[part], dst_part)

                        for db in dbs['dst']:

                            log.info("dropping partition %s['%s'] %s ",
                                     table, part, db.connection.hosts[0][0])

                            db.execute(f"alter table {dbname}.{table} drop partition id '{part}'")

                    else:
                        log.info("partition %s['%s']: MISMATCH, partition does not exist on cluster",
                                 table, part)

                    for db in dbs['src']:
                        args.append((
                            db.connection.hosts[0][0],
                            next(dst_dbs).connection.hosts[0][0],
                            dbname, table, part))


            with multiprocessing.Pool(len(dbs['dst']) if settings['poolsize'] == 'auto' else settings['poolsize']) as pool:
                _ = pool.starmap(copy_partition, args)


if __name__ == "__main__":

    with open(sys.argv[1], mode='r', encoding='utf-8') as f:
        settings = yaml.load(f, Loader=yaml.Loader)

    log = multiprocessing.log_to_stderr(logging.INFO)

    with Timing(log, 'synchronization'):
        main()
