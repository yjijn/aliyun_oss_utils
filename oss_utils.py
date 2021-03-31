import re
import oss2
import os, json
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, ALL_COMPLETED
from aliyunsdkcore import client
from aliyunsdksts.request.v20150401 import AssumeRoleRequest

# 配置
AL_OSS_URL = 'oss-cn-*****.aliyuncs.com'  # oss服务url
AL_OSS_OPTION = {
    'AK_ID': '********',  # 阿里云配置的AccessKeyId
    'AK_SE': '********',  # 阿里云配置的AccessKeySecret
    'BUCKET_NAME': '********',  # bucket名称
    'CN': 'cn-******',  # 域
    'roleArn': 'acs:ram::******:role/*********',
}

PREFERRED_SIZE = 10 * 1024  # 分片上传，单片大小


class OssOperator(object):
    '''
    oss操作类，直接使用AccessKeyId和AccessKeySecret进行链接，不安全，仅限后端服务使用
    '''
    def __init__(self, options):
        access_key_id = options['AK_ID']
        access_key_secret = options['AK_SE']
        bucket_name = options['BUCKET_NAME']
        # 阿里云主账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM账号进行API访问或日常运维，请登录RAM控制台创建RAM账号。
        self.auth = oss2.Auth(access_key_id, access_key_secret)
        # Endpoint以杭州为例，其它Region请按实际情况填写。
        self.bucket = oss2.Bucket(self.auth, 'http://' + AL_OSS_URL, bucket_name)


    def downloadFUNC(self, objectPath, localPath):  # 下载文件
        '''
        下载文件
        :param objectPath: oss对应的文件名（路径）
        :param localPath:  本地路径
        :return:
        '''
        self.bucket.get_object_to_file(objectPath, localPath)


    def overwriteFUNC(self, objectPath, localPath):  # 上传文件
        '''
        上传文件
        :param objectPath: oss对应的文件名（路径）
        :param localPath:  本地文件路径
        :return:
        '''
        # 必须以二进制的方式打开文件。
        with open(localPath, 'rb') as fileobj:
            # Seek方法用于指定从第1000个字节位置开始读写。上传时会从您指定的第1000个字节位置开始上传，直到文件结束。
            fileobj.seek(0, os.SEEK_SET)
            # Tell方法用于返回当前位置。
            current = fileobj.tell()
            self.bucket.put_object(objectPath, fileobj)


    def deleteFUNC(self, objectPath, localPath):  # 删除文件
        self.bucket.delete_object(objectPath, localPath)


    def uploadFile2Oss(self, file, objPath):  # 上传文件方法
        '''
        把文件对象上传oss
        :param file:  文件对象
        :param objPath: 上传到oss对应的文件名（路径）
        :return:
        '''
        file.seek(0, os.SEEK_SET)
        # Tell方法用于返回当前位置。
        current = file.tell()
        self.bucket.put_object(objPath, file)
        print('upload to oss server {}'.format(objPath))
        return getOssSavePath(objPath)

    def uploadFile2OssByPart(self, file, objPath):  # 分片上传方法
        '''
        把文件对象分片上传oss
        :param file:  文件对象
        :param objPath: 上传到oss对应的文件名（路径）
        :return:
        '''
        totalSize = file.size
        # determine_part_size方法用于确定分片大小。
        partSize = oss2.determine_part_size(totalSize, preferred_size=PREFERRED_SIZE)
        uploadId = self.bucket.init_multipart_upload(objPath).upload_id
        parts = []
        # 分片上传
        executor = ThreadPoolExecutor(max_workers=1)
        allTask = []
        partNumber = 1
        offset = 0
        while offset < totalSize:
            numToUpload = min(partSize, totalSize - offset)
            print(partNumber, numToUpload)
            ## 一般上传
            # 调用SizedFileAdapter(file, size)方法会生成一个新的文件对象，重新计算起始追加位置。
            result = self.bucket.upload_part(objPath, uploadId, partNumber, oss2.SizedFileAdapter(file, numToUpload))
            parts.append(oss2.models.PartInfo(partNumber, result.etag))
            ## 多线程上传
            # allTask.append(executor.submit(_uploadPart, partNumber, numToUpload))
            # offset += numToUpload
            # partNumber += 1
        # 完成分片上传。
        wait(allTask, return_when=ALL_COMPLETED)
        resultList = [future.result() for future in as_completed(allTask)]
        for data in sorted(resultList, key=lambda x: x[0]):  # 重排序按正常数据加入parts
            partNumber, result = data[0], data[1]
            print(partNumber)
            parts.append(oss2.models.PartInfo(partNumber, result.etag))

        self.bucket.complete_multipart_upload(objPath, uploadId, parts)
        return getOssSavePath(objPath)


class StsOssOperator(OssOperator):  # Sts服务，获取AccessKeyId和AccessKeySecret和临时token
    '''
    使用Sts服务操作类，比OssOperator安全，可开放给前端使用，也可以后端使用
    '''
    def __init__(self, OSS_OPTION):  # 重写init方法
        # 获取AccessKeyId和AccessKeySecret和临时token
        accessKeyId = OSS_OPTION['AK_ID']
        accessKeySecret = OSS_OPTION['AK_SE']
        self.bucketName = OSS_OPTION['BUCKET_NAME']
        cn = OSS_OPTION['CN']
        roleArn = OSS_OPTION['roleArn']
        clt = client.AcsClient(accessKeyId, accessKeySecret, cn)
        req = AssumeRoleRequest.AssumeRoleRequest()
        policyText = '{"Version": "1", "Statement": [{"Action": ["oss:PutObject", "oss:GetObject"], ' \
                      '"Effect": "Allow", "Resource": ["acs:oss:*:*:' + self.bucketName + '/*"]}]}'
        # 设置返回值格式为JSON。
        req.set_accept_format('json')
        req.set_RoleArn(roleArn)
        req.set_RoleSessionName('session-name')
        req.set_Policy(policyText)
        body = clt.do_action_with_exception(req)
        # 使用RAM账号的AccessKeyId和AccessKeySecret向STS申请临时token。
        self.token = json.loads(oss2.to_unicode(body))
        self.AccessKeyId = self.token['Credentials']['AccessKeyId']
        self.AccessKeySecret = self.token['Credentials']['AccessKeySecret']
        self.SecurityToken = self.token['Credentials']['SecurityToken']
        self._connectOss()  # 链接oss数据库

    def getAccessToken(self):  # 返回AccessKeyId和AccessKeySecret和临时token等信息，可提供给前端使用
        return self.AccessKeyId, self.AccessKeySecret, self.SecurityToken

    def _connectOss(self):  # 上传、下载操作先调用本方法，链接阿里云oss服务
        auth = oss2.StsAuth(self.AccessKeyId, self.AccessKeySecret, self.SecurityToken)
        # Endpoint以杭州为例，其它Region请按实际情况填写。
        self.bucket = oss2.Bucket(auth, 'http://' + AL_OSS_URL, self.bucketName)



# oss savePath oss服务上的保存路径
def getOssSavePath(objPath):
    '''
    获取oss对象访问的url
    :param objPath:  oss bucket 内路径
    :return: oss object的url,即使用oss服务情况下报告表的savePath字段
    '''
    return 'http://' + AL_OSS_OPTION['BUCKET_NAME'] + '.' + AL_OSS_URL + '/' + objPath


if __name__ == '__main__':
    try:
        cl = StsOssOperator(AL_OSS_OPTION)
        # cl = OssOperator(AL_OSS_OPTION)
        with open('./test.txt', 'rb') as file:
            cl.uploadFile2Oss(file, '123465.txt')
    except Exception as e:
        print('error:', e)
