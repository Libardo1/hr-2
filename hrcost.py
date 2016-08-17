# -*- coding = utf-8 -*-

import sys
import os
import pandas
import getopt
import xlrd
import yaml
import boto3

def usage():
    """
    print usage help string
    :return:
    """
    print "python "+ sys.argv[0] +"-i input_xlsx"

def getopts():
    """
    get command line options
    :return:
    """
    input_file = ""

    opts,args = getopt.getopt(sys.argv[1:],"hi:")

    for op, value in opts:
        if op == "-i":
            input_file = value
        elif op == "-h":
            usage()
            sys.exit()

    if input_file == "":
        usage()
        sys.exit()

    return input_file


def s3test():
    s3 = boto3.client('s3')
    bucket = ''
    key = ""
    response = s3.get_object(Bucket=bucket, Key=key)


def main():

    reload(sys)
    sys.setdefaultencoding('utf8')

    input_file = getopts()
    curdir = os.path.abspath(os.path.curdir)

    yaml_obj = yaml.load(open('config.yaml'))

    dirs = yaml_obj.get('dir')
    data_dir = dirs['data']
    data_dir = os.path.join(curdir,data_dir)

    pro_list_file = yaml_obj.get('projects')['file']

    colnames = yaml_obj.get('colnames')
    colxm = colnames['xm']
    colbh = colnames['lxbh']
    colmc = colnames['xmmc']
    colbm = colnames['zbbm']
    colrq = colnames['tjrq']
    colbl = colnames['fhbl']
    colzy = colnames['zybm']



    #costfile = yaml_obj.get('costs')['file']
    #workbook = xlrd.open_workbook(os.path.join(data_dir, costfile))

    workbook = xlrd.open_workbook(os.path.join(data_dir, input_file))
    costsheetname = yaml_obj.get('sheets')['cost']
    sheetcost = workbook.sheet_by_name(costsheetname)
    rowscost = sheetcost.nrows
    colscost = sheetcost.ncols
    row = sheetcost.row(0)
    dfcost = pandas.DataFrame(columns=(row[0].value, row[1].value, row[2].value, row[3].value, row[4].value, \
                                       row[5].value, row[6].value, row[7].value, row[8].value, row[9].value, \
                                       row[10].value, row[11].value, row[12].value, row[13].value, row[14].value, \
                                       row[15].value))
    dfcost2 = pandas.DataFrame(columns=(row[0].value, row[1].value, row[2].value, row[3].value, row[4].value, \
                                       row[5].value, row[6].value, row[7].value, row[8].value, row[9].value, \
                                       row[10].value, row[11].value, row[12].value, row[13].value, row[14].value, \
                                       row[15].value, yaml_obj.get('colnames')['jjxs']))
    for i in range(1, rowscost):
        row = sheetcost.row(i)
        dfcost.loc[len(dfcost)] = [row[0].value, row[1].value, row[2].value, row[3].value, row[4].value, \
                                       row[5].value, row[6].value, row[7].value, row[8].value, row[9].value, \
                                       row[10].value, row[11].value, row[12].value, row[13].value, row[14].value, \
                                       row[15].value ]

    loadsheetname = yaml_obj.get('sheets')['cost']
    sheetload = workbook.sheet_by_name(loadsheetname)
    rowsload = sheetload.nrows
    colsload = sheetload.ncols
    row = sheetload.row(0)
    dfload= pandas.DataFrame(columns=(row[0].value, row[1].value, row[2].value, row[3].value, row[4].value, \
                                       row[5].value, row[6].value, row[7].value))

    for i in range(1, rowsload):
        row = sheetload.row(i)
        dfload.loc[len(dfload)] = [row[0].value, row[1].value, row[2].value, row[3].value, row[4].value, \
                                   row[5].value, row[6].value, row[7].value]


    xmlist = list(set(dfcost[colxm].tolist()))

    for xm in xmlist:
        print xm
        dfxm = dfcost[dfcost[colxm] == xm]
        dfgz = dfxm[dfxm[yaml_obj.get('colnames')['cbzq']] != yaml_obj.get('colvalue')['bn']]
        dfjj = dfxm[dfxm[yaml_obj.get('colnames')['cbzq']] == yaml_obj.get('colvalue')['bn']]
        sumgz =  dfgz[yaml_obj.get('colnames')['hjcb']].sum()
        sumjj = dfjj[yaml_obj.get('colnames')['hjcb']].sum()
        jjbl = (sumjj+sumgz)/sumgz
        dfxm[yaml_obj.get('colnames')['jjxs']] = jjbl
        frames = [dfcost2, dfxm]
        dfcost2 = pandas.concat(frames)


    dfcost2.to_csv(os.path.join(data_dir, 'hrcost.csv'), encoding='utf-8', index=False)
    dfcost2.to_csv(os.path.join(data_dir, 'hrcost_gbk.csv'), encoding='gbk', index=False)

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    main()
