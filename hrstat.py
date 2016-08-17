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


    workbook = xlrd.open_workbook(os.path.join(data_dir, pro_list_file))
    sheet = workbook.sheet_by_name('Excel')
    rows = sheet.nrows
    cols = sheet.ncols
    dfplist = pandas.DataFrame(columns=(colbh, colmc, colbm, colxm))
    for i in range(1, rows):
        row = sheet.row(i)
        dfplist.loc[len(dfplist)] = [row[0].value, row[1].value, row[3].value,row[2].value]

    df = pandas.DataFrame(columns = (colxm, colbh, colmc, colbm, colrq, colbl, colzy))

    workbook = xlrd.open_workbook(os.path.join(data_dir,input_file))
    sheetsname = workbook.sheet_names()

    print input_file
    print workbook.nsheets
    count = 0;
    for s in sheetsname:
        print s
        sheet = workbook.sheet_by_name(s)
        rows = sheet.nrows
        cols = sheet.ncols
        n_pro = rows - 2
        n_emp = cols - 6
        print (n_pro, n_emp)

        zy = sheet.row(0)[2].value
        rq = sheet.row(0)[4].value

        for i in range(2,rows):
            rowxm = sheet.row(1)
            row = sheet.row(i)
            for j in range(6, cols):
                if type(row[j].value)is float:
                #if bl != "":
                    bl = row[j].value

                    if bl > 0.0:

                        bh = row[1].value
                        mc = row[2].value
                        bm = row[3].value
                        xm = rowxm[j].value
                        dfpro = dfplist[dfplist[colbh]==bh]
                        if not dfpro.empty:
                            mc = dfpro[colmc].max()
                            bm = dfpro[colbm].max()
                        if bm == "":
                            bm = zy
                        df.loc[len(df)] = [xm, bh, mc, bm, rq, bl, zy]
                        count += 1
                        print count
                        print xm


    print df

    df.to_csv(os.path.join(data_dir, 'hr.csv'), encoding='utf-8', index=False)
    df.to_csv(os.path.join(data_dir, 'hr_gbk.csv'), encoding='gbk', index=False)

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    main()
