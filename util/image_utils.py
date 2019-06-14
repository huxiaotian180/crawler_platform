from xml.dom.minidom import Document
import uuid
import requests
from config.config import images_conf
import os
from requests_toolbelt.multipart.encoder import MultipartEncoder
import datetime
from Crypto.Cipher import AES
import hashlib
import json
from base64 import b64encode
import mimetypes


BS = AES.block_size


def padding_pkcs5(value):
    return str.encode(value + (BS - len(value) % BS) * chr(BS - len(value) % BS))


def gen_file_xml(filepath, filename):
    doc = Document()  # 创建DOM文档对象
    data = doc.createElement('data')  # 创建根元素
    head = doc.createElement('head')
    head.setAttribute('name', "EASYSCAN")
    head.setAttribute('version', "3.3.4")
    data.appendChild(head)

    action = doc.createElement('action')
    action_text = doc.createTextNode('1008')
    action.appendChild(action_text)

    sessionid = doc.createElement('sessionid')
    sessionid_text = doc.createTextNode(str(uuid.uuid1()).replace('-', ''))
    sessionid.appendChild(sessionid_text)

    channel = doc.createElement('channel')
    channel_text = doc.createTextNode('1')
    channel.appendChild(channel_text)

    param = doc.createElement('param')

    managecom = doc.createElement('managecom')
    managecom_text = doc.createTextNode('86')
    managecom.appendChild(managecom_text)

    usercode = doc.createElement('usercode')
    usercode_text = doc.createTextNode('1008')
    usercode.appendChild(usercode_text)

    head.appendChild(action)
    head.appendChild(sessionid)
    head.appendChild(channel)
    head.appendChild(param)
    head.appendChild(managecom)
    head.appendChild(usercode)

    body = doc.createElement('body')
    files = doc.createElement('files')
    files.setAttribute('filepath', filepath)
    file = doc.createElement('file')
    file.setAttribute('filename', filename)

    files.appendChild(file)
    body.appendChild(files)
    data.appendChild(body)

    doc.appendChild(data)

    return str(Document.toxml(doc, encoding='utf-8'), encoding="utf8")


def gen_index_xml(doc_code, buss_type, sub_type, file_name, file_path):
    root = Document()
    transdata = root.createElement('TRANSDATA')
    transhead = root.createElement('TRANSHEAD')

    version = root.createElement('VERSION')
    version_text = root.createTextNode('1.0')
    version.appendChild(version_text)

    syscode = root.createElement('SYSCODE')
    syscode_text = root.createTextNode('001')
    syscode.appendChild(syscode_text)

    tranpasswd = root.createElement('TRANPASSWD')
    tranpasswd_text = root.createTextNode('111111')
    tranpasswd.appendChild(tranpasswd_text)

    transcode = root.createElement('TRANSCODE')
    transcode_text = root.createTextNode('80012')
    transcode.appendChild(transcode_text)

    tranuser = root.createElement('TRANUSER')
    tranuser_text = root.createTextNode('admin')
    tranuser.appendChild(tranuser_text)

    transbody = root.createElement('TRANSBODY')

    doc = root.createElement('DOC')
    doccode = root.createElement('DOCCODE')
    doccode_text = root.createTextNode(doc_code)
    doccode.appendChild(doccode_text)

    groupno = root.createElement('GROUPNO')
    groupno_text = root.createTextNode(doc_code)
    groupno.appendChild(groupno_text)

    channel = root.createElement('CHANNEL')
    channel_text = root.createTextNode('1')
    channel.appendChild(channel_text)

    busstype = root.createElement('BUSSTYPE')
    busstype_text = root.createTextNode(buss_type)
    busstype.appendChild(busstype_text)

    subtype = root.createElement('SUBTYPE')
    subtype_text = root.createTextNode(sub_type)
    subtype.appendChild(subtype_text)

    numpages = root.createElement('NUMPAGES')
    numpages_text = root.createTextNode('1')
    numpages.appendChild(numpages_text)

    managecom = root.createElement('MANAGECOM')
    managecom_text = root.createTextNode('86')
    managecom.appendChild(managecom_text)

    scanno = root.createElement('SCANNO')
    scanno_text = root.createTextNode('0')
    scanno.appendChild(scanno_text)

    scanoperator = root.createElement('SCANOPERATOR')
    scanoperator_text = root.createTextNode('admin001')
    scanoperator.appendChild(scanoperator_text)

    scandate = root.createElement('SCANDATE')
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    scandate_text = root.createTextNode(now)
    scandate.appendChild(scandate_text)

    page = root.createElement('PAGE')
    pageno = root.createElement('PAGENO')
    pageno_text = root.createTextNode('1')
    pageno.appendChild(pageno_text)

    pagename = root.createElement('PAGENAME')
    pagename_text = root.createTextNode(file_name)
    pagename.appendChild(pagename_text)

    pagepath = root.createElement('PAGEPATH')
    pagepath_text = root.createTextNode(file_path)
    pagepath.appendChild(pagepath_text)

    transdata.appendChild(transhead)
    transhead.appendChild(version)
    transhead.appendChild(syscode)
    transhead.appendChild(tranpasswd)
    transhead.appendChild(transcode)
    transhead.appendChild(tranuser)

    transdata.appendChild(transbody)
    transbody.appendChild(doc)
    doc.appendChild(doccode)
    doc.appendChild(groupno)
    doc.appendChild(channel)
    doc.appendChild(busstype)
    doc.appendChild(subtype)
    doc.appendChild(numpages)
    doc.appendChild(managecom)
    doc.appendChild(scanno)
    doc.appendChild(scanoperator)
    doc.appendChild(scandate)
    doc.appendChild(page)
    page.appendChild(pageno)
    page.appendChild(pagename)
    page.appendChild(pagepath)

    root.appendChild(transdata)
    return str(Document.toxml(root, encoding='utf-8'), encoding="utf8")


def upload_file(local_file_path, local_file_name, server_file_path, server_file_name):
    url = images_conf['fileUpUrl']
    ff = local_file_path + os.path.sep + local_file_name
    file_xml = gen_file_xml(server_file_path, server_file_name)

    mimetypes.init([r'../config/mimes.txt'])
    ext = os.path.splitext(local_file_name)[1]
    mtype = mimetypes.types_map[ext]

    m = MultipartEncoder(fields={'IndexXML': file_xml, 'Page': (local_file_name, open(ff, 'rb'), mtype)})

    return requests.post(url, data=m, headers={'Content-Type': m.content_type})


def upload_index(doc_code, buss_type, sub_type, file_name, file_path, key):
    index_xml = gen_index_xml(doc_code, buss_type, sub_type, file_name, file_path)
    result = sign(index_xml, key)
    json_result = json.dumps(result)
    url = images_conf['indexUpUrl']
    data = {'inputXml': json_result}
    return requests.post(url, data)


def sign(data, key):
    encrypt_data = encrypt(data, key)
    sha = hashlib.sha256()
    sha.update(encrypt_data.encode("utf8"))
    sign = sha.hexdigest()
    result = {'requestSign': sign, 'requestString': encrypt_data}
    return result


def encrypt(value, key):
    cryptor = AES.new(key, AES.MODE_ECB)
    value = b64encode(bytes(value, encoding='utf-8'))
    padding_value = padding_pkcs5(str(value, encoding='utf-8'))
    ciphertext = cryptor.encrypt(padding_value)
    return ''.join(['%02x' % i for i in ciphertext]).lower()


def upload(local_file_path, local_file_name, server_file_path, server_file_name, doc_code, buss_type, sub_type):
    s = upload_file(local_file_path, local_file_name, server_file_path, server_file_name)
    file_name = server_file_name
    file_path = server_file_path
    key = bytes.fromhex(images_conf['key'])
    s = upload_index(doc_code, buss_type, sub_type, file_name, file_path, key)
    return s


def upload_credit_reports(doc_code, buss_type, sub_type, local_file_path, local_file_name, server_file_path):
    server_file_name = local_file_name
    response = upload(local_file_path, local_file_name, server_file_path, server_file_name, doc_code, buss_type, sub_type)
    return response


def test():
    local_file_path = '/Users/ice/Desktop/报销'
    local_file_name = 'WechatIMG108.jpeg'
    server_file_path = '/reports/2019/03/18/'

    doc_code = 'BC130580'
    sub_type = '91CreditRep'
    sub_type = 'approveRep'
    sub_type = 'bairongRep'
    sub_type = 'gxbRep'
    sub_type = 'intellicreditRep'
    sub_type = 'juxinliRep'
    sub_type = 'pyratingRep'
    sub_type = 'shujumoheRep'
    sub_type = 'tianxingRep'
    buss_type = 'creditReport'
    upload_credit_reports(doc_code, buss_type, sub_type, local_file_path, local_file_name, server_file_path)

