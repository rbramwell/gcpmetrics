#!/usr/bin/env python

from __future__ import print_function

import os
import sys
import shutil
import argparse
import datetime
import yaml
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import query as gcm_v3_query

parser = argparse.ArgumentParser(
    description='Google Cloud Monitoring API Command Line\nWebsite: https://github.com/odin-public/gcpmetrics',
    formatter_class=argparse.RawDescriptionHelpFormatter
)

parser.add_argument('--version', default=None, action='store_true', help='Print gcpmetics version and exit.')
parser.add_argument('--init-config', help='Location of configuration files.', metavar='DIR')
parser.add_argument('--config', help='Local configuration *.yaml file to be used.', metavar='FILE')
parser.add_argument('--keyfile', help='Goolge Cloud Platform service account key file.', metavar='FILE')
parser.add_argument('--preset', help='Preset ID, like http_response_5xx_sum, etc.', metavar='ID')
parser.add_argument('--project', help='Project ID.', metavar='ID')
parser.add_argument('--list-resources', default=None, action='store_true', help='List monitored resource descriptors and exit.')
parser.add_argument('--list-metrics', default=None, action='store_true', help='List available metric descriptors and exit.')
parser.add_argument('--query', default=None, action='store_true', help='Run the time series query.')
parser.add_argument('--service', help='Service ID.', metavar='ID')
parser.add_argument('--metric', help='Metric ID as defined by Google Monitoring API.', metavar='ID')
parser.add_argument('--infinite', default=None, action='store_true', help='Calculate time delta since the dawn of time.')
parser.add_argument('--days', default=0, help='Days from now to calculate the query start date.', metavar='INT')
parser.add_argument('--hours', default=0, help='Hours from now to calculate the query start date.', metavar='INT')
parser.add_argument('--minutes', default=0, help='Minutes from now to calculate the query start date.', metavar='INT')
parser.add_argument('--resource-filter', default=None, help='Filter of resources in the var:val[,var:val] format.', metavar='S')
parser.add_argument('--metric-filter', default=None, help='Filter of metrics in the var:val[,var:val] format.', metavar='S')
parser.add_argument('--align', default=None, help='Alignment of data ALIGN_NONE, ALIGN_SUM. etc.', metavar='A')
parser.add_argument('--align-period-seconds', default=None, help='Alignment period in seconds. Default: 300 seconds.', metavar='A')
parser.add_argument('--reduce', default=None, help='Reduce of data REDUCE_NONE, REDUCE_SUM, etc.', metavar='R')
parser.add_argument('--reduce-grouping', default=None, help='Reduce grouping in the var1[,var2] format.', metavar='R')
parser.add_argument('--iloc00', default=None, action='store_true', help='Print value from the table index [0:0] only.')


def error(message):
    sys.stderr.write('error: {}'.format(message))
    print()
    print()
    parser.print_help()
    sys.exit(1)


def list_monitored_resource_descriptors(client, project_name):
    print('Monitored resource descriptors:')

    index = 0
    for descriptor in client.list_monitored_resource_descriptors(project_name):
        index += 1
        print('Resource descriptor #{}'.format(index))
        print('\tname: {}'.format(descriptor.name))
        print('\ttype: {}'.format(descriptor.type))
        print('\tdisplay_name: {}'.format(descriptor.display_name))
        print('\tdescription: {}'.format(descriptor.description))
        print('\tlabels:')
        subindex = 0
        for label in descriptor.labels:
            subindex += 1
            print('\t\tLabel descriptor #{}'.format(subindex))
            print('\t\t\tkey: {}'.format(label.key))
            print('\t\t\tvalue_type: {}'.format(label.value_type))
            print('\t\t\tdescription: {}'.format(label.description))
        print()


def list_metric_descriptors(client, project_name):
    print('Defined metric descriptors:')

    index = 0
    for descriptor in client.list_metric_descriptors(project_name):
        index += 1
        print('Metric descriptor #{}'.format(index))
        print('\tname: {}'.format(descriptor.name))
        print('\ttype: {}'.format(descriptor.type))
        print('\tmetric_kind: {}'.format(descriptor.metric_kind))
        print('\tvalue_type: {}'.format(descriptor.value_type))
        print('\tunit: {}'.format(descriptor.unit))
        print('\tdisplay_name: {}'.format(descriptor.display_name))
        print('\tdescription: {}'.format(descriptor.description.encode('utf-8')))
        print()


def perform_query(client, project_id, metric_id, days, hours, minutes, resource_filter, metric_filter,
                  align, align_period_seconds, reduce, reduce_grouping, iloc00):

    if (days + hours + minutes) == 0:
        error('No time interval specified. Please use --infinite or --days, --hours, --minutes')

    if not metric_id:
        error('Metric ID is required for query, please use --metric')

    query = gcm_v3_query.Query(
        client=client,
        project=project_id,
        metric_type=metric_id,
        days=days,
        hours=hours,
        minutes=minutes
    )

    if resource_filter:
        query = query.select_resources(**resource_filter)

    if metric_filter:
        query = query.select_metrics(**metric_filter)

    if align:
        if not iloc00:
            print('ALIGN: {} seconds: {}'.format(align, align_period_seconds))
        query = query.align(align, seconds=align_period_seconds)

    if reduce:
        if not iloc00:
            print('REDUCE: {} grouping: {}'.format(reduce, reduce_grouping))
        if reduce_grouping:
            query = query.reduce(reduce, *reduce_grouping)
        else:
            query = query.reduce(reduce)

    if not iloc00:
        print('QUERY: {}'.format(query.filter))

    dataframe = query.as_dataframe()

    if iloc00:
        if len(dataframe) == 0:
            # No dataset = zero
            print('0')

        else:
            # print "top left" element of the table only, asusming it's the only one left
            # see http://pandas.pydata.org/pandas-docs/stable/10min.html for details
            assert len(dataframe) == 1
            assert len(dataframe.iloc[0]) == 1
            print(dataframe.iloc[0, 0])

    else:
        # print the whole dataset
        print(dataframe.to_string())


def process(keyfile, config, project_id, list_resources, list_metrics, query, metric_id, days, hours, minutes,
            resource_filter, metric_filter, align, align_period_seconds, reduce, reduce_grouping, iloc00):

    client = None
    project_name = None

    if not project_id:
        error('--project not specified')

    if not keyfile:
        # --keyfile not specified, use interactive `gcloud auth login`
        client = monitoring_v3.MetricServiceClient()
        project_name = client.project_path(project_id)
    else:
        _file = keyfile
        # file is relative to config (if present)
        if config:
            _file = os.path.join(os.path.split(config)[0], keyfile)

        client = monitoring_v3.MetricServiceClient.from_service_account_json(_file)
        project_name = client.project_path(project_id)

    if list_resources:
        list_monitored_resource_descriptors(client, project_name)

    elif list_metrics:
        list_metric_descriptors(client, project_name)

    elif query:
        perform_query(client, project_id, metric_id, days, hours, minutes, resource_filter, metric_filter,
                      align, align_period_seconds, reduce, reduce_grouping, iloc00)

    else:
        error('No operation specified. Please choose one of --list-resources, --list-metrics, --query')


def init_config(args_dict):

    _dir = args_dict['init_config']
    if not os.path.exists(_dir):
        print('Creating folder: {}'.format(_dir))
        os.makedirs(_dir)

    _path = os.path.split(os.path.abspath(__file__))[0]
    _from = os.path.join(_path, 'config-template.yaml')
    _to = os.path.join(_dir, 'config.yaml')
    print('Creating configuration file: {}'.format(_to))

    shutil.copyfile(_from, _to)

    _path = os.path.split(os.path.abspath(__file__))[0]
    _from = os.path.join(_path, 'keyfile-template.json')
    _to = os.path.join(_dir, 'keyfile.json')
    print('Creating (empty) key file: {}'.format(_to))
    shutil.copyfile(_from, _to)

    print("Configuration created, use '--config {}' to reference it.".format(_dir))
    return 0


def apply_configs(args_dict):

    _path = os.path.split(os.path.abspath(__file__))[0]
    stream = open(os.path.join(_path, 'global.yaml'), 'r')
    global_config = yaml.load(stream, Loader=yaml.FullLoader)
    stream.close()

    local_config = {}
    if args_dict['config']:
        stream = open(args_dict['config'], 'r')
        local_config = yaml.load(stream, Loader=yaml.FullLoader)
        stream.close()

    _ret = args_dict
    for p in args_dict.keys():
        if _ret[p] is None:
            if p in local_config:
                _ret[p] = local_config[p]

    _ret = args_dict
    for p in args_dict.keys():
        if _ret[p] is None:
            if p in global_config:
                _ret[p] = global_config[p]

    if not args_dict['preset']:
        return _ret

    preset_id = args_dict['preset']

    local_preset = None
    if preset_id in local_config:
        local_preset = local_config[preset_id]

    global_preset = None
    if preset_id in global_config:
        global_preset = global_config[preset_id]

    if local_preset is None and global_preset is None:
        error('Preset {} not found in either local or global configudation files'.format(preset_id))

    if local_preset:
        for p in args_dict.keys():
            if _ret[p] is None or _ret[p] == 0:
                if p in local_preset:
                    _ret[p] = local_preset[p]

    if global_preset:
        for p in args_dict.keys():
            if _ret[p] is None:
                if p in global_preset:
                    _ret[p] = global_preset[p]

    return _ret


def version():
    _path = os.path.split(os.path.abspath(__file__))[0]
    _file = os.path.join(_path, './VERSION')
    f = open(_file, 'r')
    ver = f.read()
    f.close()
    return ver.strip()


def main():
    args_dict = vars(parser.parse_args())

    if args_dict['version']:
        print(version())
        return 0

    if args_dict['init_config']:
        return init_config(args_dict)

    args_dict = apply_configs(args_dict)

    if args_dict['infinite']:
        # October 6, 2011 = Google Cloud Platform launch date :-)
        dawn = datetime.datetime.strptime('2011-10-06', '%Y-%m-%d')
        now = datetime.datetime.utcnow()
        delta = now - dawn
        args_dict['days'] = delta.days
        args_dict['hours'] = 0
        args_dict['minutes'] = 0

    # --service {ID} extends resources filter as 'module_id:{ID}'
    if args_dict['service']:
        append = 'module_id:{}'.format(args_dict['service'])
        if args_dict['resource_filter'] is None:
            args_dict['resource_filter'] = append
        else:
            args_dict['resource_filter'] += append

    def process_filter(_filter):
        if not _filter:
            return None
        _filter = _filter.split(',')
        _ret = {}
        for res in _filter:
            key, value = res.split(':', 1)
            _ret[key] = value
        return _ret

    # data re-formatting for simpler use going forward
    resource_filter = process_filter(args_dict['resource_filter'])
    metric_filter = process_filter(args_dict['metric_filter'])

    if args_dict['align_period_seconds'] is None:
        # default to 5 minutes (300 seconds)
        args_dict['align_period_seconds'] = 300

    if args_dict['reduce_grouping']:
        args_dict['reduce_grouping'] = args_dict['reduce_grouping'].split(',')

    process(
        args_dict['keyfile'],
        args_dict['config'],
        args_dict['project'],
        args_dict['list_resources'],
        args_dict['list_metrics'],
        args_dict['query'],
        args_dict['metric'],
        int(args_dict['days']),
        int(args_dict['hours']),
        int(args_dict['minutes']),
        resource_filter,
        metric_filter,
        args_dict['align'],
        int(args_dict['align_period_seconds']),
        args_dict['reduce'],
        args_dict['reduce_grouping'],
        args_dict['iloc00']
    )


if __name__ == '__main__':
    sys.exit(main())
