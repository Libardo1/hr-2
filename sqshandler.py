# -*- coding = utf8 -*-

import boto3
import sys
import time
import yaml
import zipfile
import os
import xlrd
import pandas



session = boto3.Session(profile_name='hr-stat')

# Get the service resource
s3 = session.resource('s3')
sqs = session.resource('sqs')

# Get the queue
queue = sqs.get_queue_by_name(QueueName='hr-stat')



def sendmessage(Body= ""):

    now = int(time.time())

    timeArray = time.localtime(now)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

    body = "Delete sqsposter "+Body + " @ " + otherStyleTime

    queue.send_message(MessageBody=body, MessageAttributes={

        'sqshandler': {
            'StringValue': 'sqshandler',
            'DataType': 'String'
        }
        ,

        'Timer': {
            'StringValue': str(60),
            'DataType': 'String'
        }

    })


def calc_work_xlsx( BucketName , ObjKey , xlsxfile ):


    yaml_obj = yaml.load(open('config.yaml'))
    tmp_dir = yaml_obj.get('dir')['tmp']
    data_dir = yaml_obj.get('dir')['data']
    cal_work_dir = yaml_obj.get('dir')['calc-work']

    if xlsxfile == "":

        bucket = s3.Bucket(BucketName)
        obj = s3.Object(BucketName,ObjKey)

        path = obj.key.split("/")
        filename = path[len(path)-1]
        xlsxpath = os.path.join(tmp_dir, filename)
        obj.download_file(xlsxpath)

    elif xlsxfile != "":
        xlsxpath = xlsxfile

    if xlsxpath != "":
        print xlsxpath

        if True:
        #try:

            workbook = xlrd.open_workbook(xlsxpath)

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['divlist']['sheetname'])
            rows = sheet.nrows
            cols = sheet.ncols
            coldiv = yaml_obj.get('sheets')['divlist']['colnames']['bm']
            colsx = yaml_obj.get('sheets')['divlist']['colnames']['sx']


            dfdivlist = pandas.DataFrame(columns=(coldiv, colsx))
            for i in range(1, rows):
                row = sheet.row(i)
                dfdivlist.loc[len(dfdivlist)] = [row[1].value, row[2].value]

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['prolist']['sheetname'])
            rows = sheet.nrows
            cols = sheet.ncols
            colbh = yaml_obj.get('sheets')['prolist']['colnames']['lxbh']
            colmc = yaml_obj.get('sheets')['prolist']['colnames']['xmmc']
            colbm = yaml_obj.get('sheets')['prolist']['colnames']['zbbm']

            dfprolist = pandas.DataFrame(columns=(colbh, colmc, colbm))

            for i in range(1, rows):
                row = sheet.row(i)
                dfprolist.loc[len(dfprolist)] = [row[0].value, row[1].value, row[3].value]

            colxm = yaml_obj.get('sheets')['loadcalc']['colnames']['xm']
            colrq = yaml_obj.get('sheets')['loadcalc']['colnames']['tjrq']
            colbl = yaml_obj.get('sheets')['loadcalc']['colnames']['fhbl']
            colrr = yaml_obj.get('sheets')['loadcalc']['colnames']['gzrr']
            colzy = yaml_obj.get('sheets')['loadcalc']['colnames']['zybm']

            df = pandas.DataFrame(columns=(colxm, colbh, colmc, colbm, colrq, colbl, colrr, colzy))

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['loadpercent']['sheetname'])
            sheet2 = workbook.sheet_by_name(yaml_obj.get('sheets')['loadmandays']['sheetname'])

            rows = sheet.nrows
            cols = sheet.ncols
            print yaml_obj.get('sheets')['loadpercent']['row_offset']


            row_offset = int(yaml_obj.get('sheets')['loadpercent']['row_offset'])
            col_offset = int(yaml_obj.get('sheets')['loadpercent']['col_offset'])

            n_pro = rows - row_offset
            n_emp = cols - col_offset

            print (n_pro, n_emp)

            zy = sheet.row(0)[int(yaml_obj.get('sheets')['loadpercent']['bm_col'])].value
            rq = sheet.row(0)[int(yaml_obj.get('sheets')['loadpercent']['rq_col'])].value
            dfdiv = dfdivlist[dfdivlist[coldiv] == zy]
            if not dfdiv.empty:
                divsx = dfdiv[colsx].max()
            else:
                divsx = zy

            rowxm = sheet.row(1)

            for i in range(row_offset, rows):

                row = sheet.row(i)
                row2 = sheet2.row(i)
                for j in range(col_offset, cols):
                    if type(row[j].value) is float:
                        # if bl != "":
                        bl = row[j].value
                        rr = row2[j].value

                        if rr > 0:

                            bh = row[1].value
                            mc = row[2].value
                            bm = row[3].value
                            xm = rowxm[j].value
                            dfpro = dfprolist[dfprolist[colbh] == bh]
                            if not dfpro.empty:
                                mc = dfpro[colmc].max()
                                bm = dfpro[colbm].max()
                            if bm == "":
                                bm = zy
                            df.loc[len(df)] = [xm, bh, mc, bm, rq, bl, rr, zy]

            csvname = "work-" + rq + "-" + divsx + ".csv"
            csvpath = os.path.join(cal_work_dir, csvname)
            df.to_csv(csvpath, encoding='utf8', index=False)

            data = open(csvpath, 'rb')
            s3key = "calc/work/" + csvname
            file_obj = s3.Bucket(BucketName).put_object(Key=s3key, Body=data)
        """
        except Exception as e:
            print(e)
            raise e
        """

def calc_work_zip(BucketName , ObjKey):
    print "calc_work_zip"

    bucket = s3.Bucket(BucketName)
    obj = s3.Object(BucketName,ObjKey)

    yaml_obj = yaml.load(open('config.yaml'))
    tmp_dir = yaml_obj.get('dir')['tmp']
    data_dir = yaml_obj.get('dir')['data']

    #path = obj.key.split("/")

    filename = ObjKey[len("upload/work/"):]
    print filename
    tmpfile = os.path.join(tmp_dir, filename)
    obj.download_file(tmpfile)



    zFile = zipfile.ZipFile(tmpfile, 'r')
    for filename in zFile.namelist():
        print filename

        if filename[len(filename)-1] != "/" and filename[0] != "_":
            data = zFile.read(filename)
            file = open(os.path.join(tmp_dir, filename), 'w+b')
            file.write(data)
            file.close()


            calc_work_xlsx(BucketName , ObjKey, file.name)

            """
            workbook = xlrd.open_workbook(file.name)

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['divlist']['sheetname'])
            rows = sheet.nrows
            cols = sheet.ncols
            coldiv = yaml_obj.get('sheets')['divlist']['colnames']['bm']
            colsx = yaml_obj.get('sheets')['divlist']['colnames']['sx']


            dfdivlist = pandas.DataFrame(columns=(coldiv, colsx))
            for i in range(1, rows):
                row = sheet.row(i)
                dfdivlist.loc[len(dfdivlist)] = [row[1].value, row[2].value]

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['prolist']['sheetname'])
            rows = sheet.nrows
            cols = sheet.ncols
            colbh = yaml_obj.get('sheets')['prolist']['colnames']['lxbh']
            colmc = yaml_obj.get('sheets')['prolist']['colnames']['xmmc']
            colbm = yaml_obj.get('sheets')['prolist']['colnames']['zbbm']

            dfprolist = pandas.DataFrame(columns=(colbh, colmc, colbm))

            for i in range(1, rows):
                row = sheet.row(i)
                dfprolist.loc[len(dfprolist)] = [row[0].value, row[1].value, row[3].value]

            colxm = yaml_obj.get('sheets')['loadcalc']['colnames']['xm']
            colrq = yaml_obj.get('sheets')['loadcalc']['colnames']['tjrq']
            colbl = yaml_obj.get('sheets')['loadcalc']['colnames']['fhbl']
            colzy = yaml_obj.get('sheets')['loadcalc']['colnames']['zybm']

            df = pandas.DataFrame(columns=(colxm, colbh, colmc, colbm, colrq, colbl, colzy))

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['loadpercent']['sheetname'])

            rows = sheet.nrows
            cols = sheet.ncols
            print yaml_obj.get('sheets')['loadpercent']['row_offset']


            row_offset = int(yaml_obj.get('sheets')['loadpercent']['row_offset'])
            col_offset = int(yaml_obj.get('sheets')['loadpercent']['col_offset'])

            n_pro = rows - row_offset
            n_emp = cols - col_offset

            print (n_pro, n_emp)

            zy = sheet.row(0)[int(yaml_obj.get('sheets')['loadpercent']['bm_col'])].value
            rq = sheet.row(0)[int(yaml_obj.get('sheets')['loadpercent']['rq_col'])].value
            dfdiv = dfdivlist[dfdivlist[coldiv] == zy]
            if not dfdiv.empty:
                divsx = dfdiv[colsx].max()
            else:
                divsx = zy

            rowxm = sheet.row(1)

            for i in range(row_offset, rows):

                row = sheet.row(i)
                for j in range(col_offset, cols):
                    if type(row[j].value) is float:
                        # if bl != "":
                        bl = row[j].value

                        if bl > 0.0:

                            bh = row[1].value
                            mc = row[2].value
                            bm = row[3].value
                            xm = rowxm[j].value
                            dfpro = dfprolist[dfprolist[colbh] == bh]
                            if not dfpro.empty:
                                mc = dfpro[colmc].max()
                                bm = dfpro[colbm].max()
                            if bm == "":
                                bm = zy
                            df.loc[len(df)] = [xm, bh, mc, bm, rq, bl, zy]

            csvname = rq + "-" + divsx + ".csv"
            csvpath = os.path.join(data_dir, csvname)
            df.to_csv(csvpath, encoding='utf-8', index=False)

            data = open(csvpath, 'rb')
            s3key = "calc/work/" + csvname
            file_obj = s3.Bucket(Bucket).put_object(Key=s3key, Body=data)

            """





def stat_work(bucket_name):

    yaml_obj = yaml.load(open('config.yaml'))
    calc_work_dir = yaml_obj.get('dir')['calc-work']
    stat_work_dir = yaml_obj.get('dir')['stat-work']
    calc_work_path = os.path.join(os.path.abspath(os.path.curdir), calc_work_dir)
    stat_work_path = os.path.join(os.path.abspath(os.path.curdir), stat_work_dir)

    stat_csv = os.path.join(stat_work_path, "stat-work.csv")
    stat_csv_gbk = os.path.join(stat_work_path, "stat-work-gbk.csv")

    list_csv = os.listdir(calc_work_path)
    for csv in list_csv:
        if csv[0] == "." or csv[len(csv)-4:] != ".csv":
            list_csv.remove(csv)
    print list_csv
    ncsv = len(list_csv)
    if ncsv > 0:
        csvpath = os.path.join(calc_work_path, list_csv[0])
        print csvpath
        dframe = pandas.read_csv(csvpath)
        print dframe

        if ncsv > 1:
            for i in range(1, ncsv):
                csvpath = os.path.join(calc_work_path, list_csv[i])
                print csvpath

                df_tmp = pandas.read_csv(csvpath)
                data = [dframe, df_tmp]
                dframe = pandas.concat(data)
                print dframe


        dframe.to_csv(stat_csv, encoding='utf8', index=False)
        data = open(stat_csv, 'rb')
        s3key = "stat/work/stat-work.csv"
        file_obj = s3.Bucket(bucket_name).put_object(Key=s3key, Body=data)

        dframe.to_csv(stat_csv_gbk, encoding='gbk', index=False)
        data = open(stat_csv, 'rb')
        s3key = "stat/work/stat-work-gbk.csv"
        file_obj = s3.Bucket(bucket_name).put_object(Key=s3key, Body=data)


def calc_cost_xlsx(Bucket = "", key = ""):
    bucket = s3.Bucket(Bucket)
    obj = s3.Object(Bucket,key)

    yaml_obj = yaml.load(open('config.yaml'))
    tmp_dir = yaml_obj.get('dir')['tmp']
    data_dir = yaml_obj.get('dir')['data']

    path = obj.key.split("/")
    filename = path[len(path)-1]
    tmpfile = os.path.join(tmp_dir, filename)
    obj.download_file(tmpfile)

    zFile = zipfile.ZipFile(tmpfile, 'r')
    for filename in zFile.namelist():
        print filename

        if filename[len(filename)-1] != "/" and filename[0] != "_":
            data = zFile.read(filename)
            file = open(os.path.join(tmp_dir, filename), 'w+b')
            file.write(data)
            file.close()
            workbook = xlrd.open_workbook(file.name)

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['divlist']['sheetname'])
            rows = sheet.nrows
            cols = sheet.ncols
            coldiv = yaml_obj.get('sheets')['divlist']['colnames']['bm']
            colsx = yaml_obj.get('sheets')['divlist']['colnames']['sx']


            dfdivlist = pandas.DataFrame(columns=(coldiv, colsx))
            for i in range(1, rows):
                row = sheet.row(i)
                dfdivlist.loc[len(dfdivlist)] = [row[1].value, row[2].value]

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['prolist']['sheetname'])
            rows = sheet.nrows
            cols = sheet.ncols
            colbh = yaml_obj.get('sheets')['prolist']['colnames']['lxbh']
            colmc = yaml_obj.get('sheets')['prolist']['colnames']['xmmc']
            colbm = yaml_obj.get('sheets')['prolist']['colnames']['zbbm']

            dfprolist = pandas.DataFrame(columns=(colbh, colmc, colbm))

            for i in range(1, rows):
                row = sheet.row(i)
                dfprolist.loc[len(dfprolist)] = [row[0].value, row[1].value, row[3].value]

            colxm = yaml_obj.get('sheets')['loadcalc']['colnames']['xm']
            colrq = yaml_obj.get('sheets')['loadcalc']['colnames']['tjrq']
            colbl = yaml_obj.get('sheets')['loadcalc']['colnames']['fhbl']
            colzy = yaml_obj.get('sheets')['loadcalc']['colnames']['zybm']

            df = pandas.DataFrame(columns=(colxm, colbh, colmc, colbm, colrq, colbl, colzy))

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['loadpercent']['sheetname'])

            rows = sheet.nrows
            cols = sheet.ncols
            print yaml_obj.get('sheets')['loadpercent']['row_offset']


            row_offset = int(yaml_obj.get('sheets')['loadpercent']['row_offset'])
            col_offset = int(yaml_obj.get('sheets')['loadpercent']['col_offset'])

            n_pro = rows - row_offset
            n_emp = cols - col_offset

            print (n_pro, n_emp)

            zy = sheet.row(0)[int(yaml_obj.get('sheets')['loadpercent']['bm_col'])].value
            rq = sheet.row(0)[int(yaml_obj.get('sheets')['loadpercent']['rq_col'])].value
            dfdiv = dfdivlist[dfdivlist[coldiv] == zy]
            if not dfdiv.empty:
                divsx = dfdiv[colsx].max()
            else:
                divsx = zy

            rowxm = sheet.row(1)

            for i in range(row_offset, rows):

                row = sheet.row(i)
                for j in range(col_offset, cols):
                    if type(row[j].value) is float:
                        # if bl != "":
                        bl = row[j].value

                        if bl > 0.0:

                            bh = row[1].value
                            mc = row[2].value
                            bm = row[3].value
                            xm = rowxm[j].value
                            dfpro = dfprolist[dfprolist[colbh] == bh]
                            if not dfpro.empty:
                                mc = dfpro[colmc].max()
                                bm = dfpro[colbm].max()
                            if bm == "":
                                bm = zy
                            df.loc[len(df)] = [xm, bh, mc, bm, rq, bl, zy]

            csvname = rq + "-" + divsx + ".csv"
            csvpath = os.path.join(data_dir, csvname)
            df.to_csv(csvpath, encoding='utf-8', index=False)

            data = open(csvpath, 'rb')
            s3key = "calc/work/" + csvname
            file_obj = s3.Bucket(Bucket).put_object(Key=s3key, Body=data)

def calc_cost_zip(Bucket = "", key = ""):
    bucket = s3.Bucket(Bucket)
    obj = s3.Object(Bucket,key)

    yaml_obj = yaml.load(open('config.yaml'))
    tmp_dir = yaml_obj.get('dir')['tmp']
    data_dir = yaml_obj.get('dir')['data']

    path = obj.key.split("/")
    filename = path[len(path)-1]
    tmpfile = os.path.join(tmp_dir, filename)
    obj.download_file(tmpfile)

    zFile = zipfile.ZipFile(tmpfile, 'r')
    for filename in zFile.namelist():
        print filename

        if filename[len(filename)-1] != "/" and filename[0] != "_":
            data = zFile.read(filename)
            file = open(os.path.join(tmp_dir, filename), 'w+b')
            file.write(data)
            file.close()
            workbook = xlrd.open_workbook(file.name)

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['divlist']['sheetname'])
            rows = sheet.nrows
            cols = sheet.ncols
            coldiv = yaml_obj.get('sheets')['divlist']['colnames']['bm']
            colsx = yaml_obj.get('sheets')['divlist']['colnames']['sx']


            dfdivlist = pandas.DataFrame(columns=(coldiv, colsx))
            for i in range(1, rows):
                row = sheet.row(i)
                dfdivlist.loc[len(dfdivlist)] = [row[1].value, row[2].value]

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['prolist']['sheetname'])
            rows = sheet.nrows
            cols = sheet.ncols
            colbh = yaml_obj.get('sheets')['prolist']['colnames']['lxbh']
            colmc = yaml_obj.get('sheets')['prolist']['colnames']['xmmc']
            colbm = yaml_obj.get('sheets')['prolist']['colnames']['zbbm']

            dfprolist = pandas.DataFrame(columns=(colbh, colmc, colbm))

            for i in range(1, rows):
                row = sheet.row(i)
                dfprolist.loc[len(dfprolist)] = [row[0].value, row[1].value, row[3].value]

            colxm = yaml_obj.get('sheets')['loadcalc']['colnames']['xm']
            colrq = yaml_obj.get('sheets')['loadcalc']['colnames']['tjrq']
            colbl = yaml_obj.get('sheets')['loadcalc']['colnames']['fhbl']
            colzy = yaml_obj.get('sheets')['loadcalc']['colnames']['zybm']

            df = pandas.DataFrame(columns=(colxm, colbh, colmc, colbm, colrq, colbl, colzy))

            sheet = workbook.sheet_by_name(yaml_obj.get('sheets')['loadpercent']['sheetname'])

            rows = sheet.nrows
            cols = sheet.ncols
            print yaml_obj.get('sheets')['loadpercent']['row_offset']


            row_offset = int(yaml_obj.get('sheets')['loadpercent']['row_offset'])
            col_offset = int(yaml_obj.get('sheets')['loadpercent']['col_offset'])

            n_pro = rows - row_offset
            n_emp = cols - col_offset

            print (n_pro, n_emp)

            zy = sheet.row(0)[int(yaml_obj.get('sheets')['loadpercent']['bm_col'])].value
            rq = sheet.row(0)[int(yaml_obj.get('sheets')['loadpercent']['rq_col'])].value
            dfdiv = dfdivlist[dfdivlist[coldiv] == zy]
            if not dfdiv.empty:
                divsx = dfdiv[colsx].max()
            else:
                divsx = zy

            rowxm = sheet.row(1)

            for i in range(row_offset, rows):

                row = sheet.row(i)
                for j in range(col_offset, cols):
                    if type(row[j].value) is float:
                        # if bl != "":
                        bl = row[j].value

                        if bl > 0.0:

                            bh = row[1].value
                            mc = row[2].value
                            bm = row[3].value
                            xm = rowxm[j].value
                            dfpro = dfprolist[dfprolist[colbh] == bh]
                            if not dfpro.empty:
                                mc = dfpro[colmc].max()
                                bm = dfpro[colbm].max()
                            if bm == "":
                                bm = zy
                            df.loc[len(df)] = [xm, bh, mc, bm, rq, bl, zy]

            csvname = rq + "-" + divsx + ".csv"
            csvpath = os.path.join(data_dir, csvname)
            df.to_csv(csvpath, encoding='utf-8', index=False)

            data = open(csvpath, 'rb')
            s3key = "calc/work/" + csvname
            file_obj = s3.Bucket(Bucket).put_object(Key=s3key, Body=data)

            #df.to_csv(os.path.join(data_dir, 'hr_gbk.csv'), encoding='gbk', index=False)



def queuehandler():
    # Process messages by printing out body and optional author name
    while True:
        messages = queue.receive_messages(MessageAttributeNames=['Function','Bucket'])
        now = int(time.time())
        timeArray = time.localtime(now)
        otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

        #

        if not len(messages) > 0 :

            print "break " + str(len(messages)) + " @ " + otherStyleTime

            break

        for message in messages:
            print "receive " + str(len(messages)) + " @ " + otherStyleTime

            #try:
            if True:
                func_name = message.message_attributes['Function']['StringValue']
                bucket_name = message.message_attributes['Bucket']['StringValue']
                object_key = message.body
                print (func_name, bucket_name, object_key)

                if func_name == "upload-work":
                    print func_name

                    if object_key[len(object_key)-4:] == ".zip":
                        print "zip"
                        calc_work_zip(bucket_name, object_key)
                    elif object_key[len(object_key)-5:] == ".xlsx":
                        print "xlsx"
                        calc_work_xlsx(bucket_name, object_key, "")

                    stat_work(bucket_name)

                elif func_name == "upload-cost":
                    print func_name
                    if object_key[len(object_key) - 4:] == ".zip":
                        print "zip"
                        calc_cost_zip(bucket_name, object_key)
                    elif object_key[len(object_key) - 5:] == ".xlsx":
                        print "xlsx"
                        calc_cost_xlsx(bucket_name, object_key)
                else:
                    print "NULL"


                #print (func_name,  object_key)

                #print message.message_attributes

            """
            except Exception as e:
                print(e)
                raise e
            """
            message.delete()


def main():
    #reload(sys)
    #sys.setdefaultencoding('utf8')

    while True:
        queuehandler()
        yaml_obj = yaml.load(open('config.yaml'))
        timer = int(yaml_obj.get('timer')['sqshandler'])
        #print timer
        time.sleep(timer)

if __name__ == '__main__':
    main()