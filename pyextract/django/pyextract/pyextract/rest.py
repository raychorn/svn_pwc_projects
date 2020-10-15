from django.template.loader import get_template
from django.template import Context
from django.http import HttpResponse, HttpResponseNotFound, HttpResponseNotAllowed
from django.conf import settings

import os
import psutil
import uuid

import json

import pyotp

__row_template__ = '<tr><td align="left"><b><nobr>%s</nobr></b></td><td align="left"><b><nobr>%s</nobr></b></td></tr>'

__span_template__ = '<span class="greenbox">%s</span>'


def get_epochtime_ms():
    import datetime
    return round(datetime.datetime.utcnow().timestamp() * 1000)


def get_delta_secs(ts, ts_begin=None):
    ts_begin = ts if (ts_begin is None) else ts_begin
    return int((max(ts, ts_begin) - min(ts, ts_begin)) / 1000)


def set_in_session(request, key, value=None):
    from tinydb import TinyDB, Query    
    from tinydb.storages import JSONStorage
    from tinydb.middlewares import CachingMiddleware

    sessionid = request.session.session_key
    root = settings.BASE_DIR
    fpath = os.sep.join([root, 'sessions'])
    if (not os.path.exists(fpath)):
        os.mkdir(fpath)
    fname = os.sep.join([fpath, sessionid + '.json'])

    db = TinyDB(fname, storage=CachingMiddleware(JSONStorage))

    __is__ = False
    aQuery = Query()
    result = db.search(aQuery.key == key)
    if (len(result) == 0):
        db.insert({'key': key, 'value': value, 'ttl' : get_epochtime_ms()})
    else:
        __is__ = True
        db.update({'key': key, 'value': value, 'ttl' : get_epochtime_ms()})
    
    db.close()

    return __is__


def get_from_session(request, key, value=None):
    from tinydb import TinyDB, Query    
    from tinydb.storages import JSONStorage
    from tinydb.middlewares import CachingMiddleware

    sessionid = request.session.session_key
    root = settings.BASE_DIR
    fpath = os.sep.join([root, 'sessions'])
    if (not os.path.exists(fpath)):
        os.mkdir(fpath)
    fname = os.sep.join([fpath, sessionid + '.json'])

    db = TinyDB(fname, storage=CachingMiddleware(JSONStorage))

    __value__ = None
    aQuery = Query()
    result = db.search(aQuery.key == key)
    if (len(result) > 0):
        __value__ = result[0].get('value')
        ts_now = get_epochtime_ms()
        __ttl__ = result[0].get('ttl', get_epochtime_ms())
        ts_delta = get_delta_secs(ts_now, ts_begin=__ttl__)
        if (ts_delta >= settings.SESSION_TTL):
            __value__ = value
            db.close()
            os.remove(fname)
            return __value__
    
    db.close()

    return __value__


def any_files_in_directory_tree(top, fext='.ecf'):
    for dirName, subdirList, fileList in os.walk(top):
        try:
            files = [f for f in fileList if (os.path.splitext(f)[-1] == fext)]
        except Exception as ex:
            files = []
        if (len(files) > 0):
            return True
    return False

def wrap_span_around(content, color='green'):
    return __span_template__ % (content)

def render_directory_contents(fp, fext='.ecf', URI='/choose_ecf_file', cachebuster=str(uuid.uuid4())):
    response = []
    try:
        files = [f for f in os.listdir(path=fp) if (os.path.splitext(f)[-1] == fext) or (os.path.isdir(os.sep.join([fp, f])))]
    except:
        files = []
    for f in files:
        __f__ = os.sep.join([fp, f])
        if (os.path.isdir(__f__)):
            __is__ = any_files_in_directory_tree(__f__, fext=fext)
            btn_title = 'Click this button to find the %s files.' % ('*'+fext) if (__is__) else ''
            content = '<button class="btn" onclick="window.location.href=\'%s?top=%s&cb=%s\'" title="%s">%s</button>' % (URI, os.sep.join([fp, f]).replace(os.sep, '/'), cachebuster, btn_title, f)   # <i class="fa fa-folder"></i>
            if (__is__):
                content = wrap_span_around(content)
            response.append(__row_template__ % (content, ''))
        else:
            content = '<a href="%s?ecf_file=%s">%s</a>' % (URI, os.sep.join([fp, f]), f)
            #content = wrap_span_around(content)
            response.append(__row_template__ % (content, ''))
    return ''.join(response)

def render_drives(cwd=None, URI='/choose_ecf_file', cachebuster=str(uuid.uuid4())):
    response = []
    drives = psutil.disk_partitions(all=True)
    for d in drives:
        f = d.device
        try:
            files = os.listdir(f)
        except:
            files = []
        if (cwd) and (not cwd.startswith(f)) and (len(files) > 0):
            item = '<a class="ui-button ui-widget ui-corner-all" href="%s?top=%s">%s</a>' % (URI, f, f)
        else:
            item = '<b>%s</b>' % (f)
        response.append(item)
    return '&nbsp;|&nbsp;'.join(response)


def is_token_valid(token):
    SECRET_KEY = ''.join([ch for ch in settings.SECRET_KEY if (str(ch).isalpha())])[0:16]
    totp = pyotp.TOTP(SECRET_KEY, digits=settings.OTP_CODE_LENGTH, interval=settings.OTP_CODE_INTERVAL)
    __is__ = totp.verify(token)
    return __is__


class TokenRequired(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args):
            request = args[0]
            try:
                is_method_get = str(request.method).upper() == 'GET'
                is_method_post = str(request.method).upper() == 'POST'
                vector = request.GET if (is_method_get) else request.POST
                authcode = vector.get(settings.OTP_CODE_VARIABLE, default='0'*settings.OTP_CODE_LENGTH)
                __is__ = is_token_valid(authcode)
                print('DEBUG.2: authcode = %s (%s)' % (authcode, __is__))
            except:
                __is__ = False
            
            if (__is__):
                return f(*args)
            else:
                return HttpResponseNotAllowed([request.method])
        return wrapped_f
    
    
class LoginRequired(object):

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args):
            request = args[0]
            try:
                is_method_get = str(request.method).upper() == 'GET'
                is_method_post = str(request.method).upper() == 'POST'
                vector = request.GET if (is_method_get) else request.POST
                login_token_valid = get_from_session(request, 'login_token_valid', value=False)
                referer = request.META.get('HTTP_REFERER')
                http_host = request.META.get('HTTP_HOST')
                print('DEBUG.3: login_token_valid = %s for %s%s from %s' % (login_token_valid, http_host, request.path, referer))
            except Exception as ex:
                login_token_valid = False
            
            if (login_token_valid):
                return f(*args)
            else:
                tplate = get_template('user_login.html')
                c = {}
                response = tplate.render(c)
                return HttpResponse(content=response)
        return wrapped_f
    
    
class SameDomainRequired(object):
    '''
    This checks from the request's ip address but not the request's port because the port will be the same as the
    HTTP_HOST by definition as to how TCP/IP works.  This, however, blocks requests from other than the same
    IP address where the server lives; blocks cross-domain requests.
    '''

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args):
            request = args[0]
            try:
                from ipware.ip import get_ip
                user_ip = request.META.get('HTTP_X_FORWARDED_FOR', request.META.get('REMOTE_ADDR', get_ip(request))).split(':')[0]
                referer = request.META.get('HTTP_REFERER', '')
                http_host = request.META.get('HTTP_HOST', '')
                if (user_ip == http_host.split(':')[0]) and (referer.find(http_host) > -1):
                    return f(*args)
                else:
                    return HttpResponseNotAllowed([request.method])
            except:
                return HttpResponseNotAllowed([request.method])
        return wrapped_f
    
    
def read_ecf_file(request):
    from ecfreader import read_encrypted_json
    fname = request.GET.get('filename', default=None)
    authcode = request.GET.get('authcode', default=None)
    SECRET_KEY = ''.join([ch for ch in settings.SECRET_KEY if (str(ch).isalpha())])[0:16]

    totp = pyotp.TOTP(SECRET_KEY, digits=settings.OTP_CODE_LENGTH, interval=settings.OTP_CODE_INTERVAL)
    __is__ = totp.verify(authcode)

    d = {}
    d['status'] = 'OK' if (fname and os.path.exists(fname) and os.path.isfile(fname)) else 'NOT_OK'
    d['filename'] = fname
    try:
        d['json_status'] = {}
        d['json'] = read_encrypted_json(fname, status=d['json_status'])
    except Exception as ex:
        d['exception'] = str(ex)
    response = json.dumps(d)
    return HttpResponse(content=response, content_type="application/json")


def rest_token():
    SECRET_KEY = ''.join([ch for ch in settings.SECRET_KEY if (str(ch).isalpha())])[0:16]
    totp = pyotp.TOTP(SECRET_KEY, digits=settings.OTP_CODE_LENGTH, interval=settings.OTP_CODE_INTERVAL)
    return totp.now()


@SameDomainRequired()
def get_rest_token(request):
    '''
    There REST method has no security because it is used to obtain a temporal token.  Tokens must be used
    within the defined interval or they become invalid.  Those wishing to use the secured ReST calls will need 
    to know about this REST call and use it appropriately.
    
    There is a hidden features, well it's not hidden here but you get the idea, this function is also a gateway.
    What is a gateway?  A gateway is something one can use to get from one place to another, like a portal
    from one domain into another.  Oh, darn, I used the word "domain" but this word has nothing to do with
    HTTP domains.  Getting back to the topic.  This gateway allows the programmer to get a token and then use
    it via a single REST Call.  Typically one would have to first get a token and then use it fast enough, say within
    30 seconds.  This gateway allows the token to be obtained and then used right away.  This can only be done
    from the same HTTP Domain (there's that "domain" word again, but this time it really does mean HTTP Domain); 
    those wishing to use this Gateway from another domain will sadly not be able to and thus the Security Model.
    '''
    d = {}

    is_method_get = str(request.method).upper() == 'GET'
    is_method_post = str(request.method).upper() == 'POST'
    vector = request.GET if (is_method_get) else request.POST
    
    uri = vector.get('uri', None)
    payload = vector.get('payload', None)
    
    if (uri) and (not payload):
        import re
            
        payload = {}
        __payload__ = {}
        reobj = re.compile(r"(.*)\[(.*)\]")
        for k in vector.keys():
            match = reobj.search(k)
            if (match and (match.group(2) not in [settings.OTP_CODE_VARIABLE])):
                if (not __payload__.get(match.group(1), None)):
                    __payload__[match.group(1)] = {}
                __payload__[match.group(1)][match.group(2)] = vector[k]
                
        if (len(__payload__.keys()) == 1):
            payload = __payload__.get(list(__payload__.keys())[0], payload)
    
    the_token = rest_token()
    if ( (uri is not None) and (payload is not None) ):
        import requests
        preamble = '/' if (not str(uri).startswith('/')) else ''
        url = 'http://' + request.META['HTTP_HOST'] + preamble + uri
        payload[settings.OTP_CODE_VARIABLE] = the_token
        response = requests.post(url, data=payload)

        try:
            if (response.ok):
                response = json.dumps(response.json())
            else:
                response.raise_for_status()
        except Exception as ex:
            print(str(ex))
            return HttpResponseNotAllowed([request.method])
    else:
        d['token'] = the_token
        response = json.dumps(d)

    return HttpResponse(content=response, content_type="application/json")
        

@TokenRequired()
def save_ecf_connection_file(request):
    '''
{
  "connection_hostname": "STRL069063.mso.net",
  "connection_username": "udx",
  "connection_portnumber": "1521",
  "connection_password": "udx",
  "connection_systemid": "VIS01",
  "ecfdirname": "//Mac/Home/Documents/GitHub/pyextract/ecfs",
  "connection_prefix": "oracle",
  "connection_type_select": "Load Balanced Connection"
}    
    '''
    data = {}
    for k in request.POST.keys():
        if (k not in [settings.OTP_CODE_VARIABLE]):
            data[k] = request.POST[k]
    dirname = data.get('ecfdirname', None)
    __is__ = False
    if (dirname):
        dirname = dirname.replace('/', os.sep)
        __is__ = os.path.exists(dirname) and os.path.isdir(dirname)
        
    d = {}
    if (__is__):
        fname = os.sep.join([dirname, data.get('connection_prefix', 'unknown') + '_' + data.get('connection_systemid', 'unknown') + '.connection'])
        
        try:
            some_json = json.dumps(data)
    
            with open(fname, 'w') as outfile:
                json.dump(data, outfile)
        except:
            __is__ = False

    d['status'] = 'OK' if (__is__) else 'NOT_OK'
    d['fname'] = fname
    try:
        d['json_status'] = {}
    except Exception as ex:
        d['exception'] = str(ex)
    response = json.dumps(d)
    return HttpResponse(content=response, content_type="application/json")


@TokenRequired()
def read_ecf_connection_file(request):
    '''
{
  "connection_hostname": "STRL069063.mso.net",
  "connection_username": "udx",
  "connection_portnumber": "1521",
  "connection_password": "udx",
  "connection_systemid": "VIS01",
  "ecfdirname": "//Mac/Home/Documents/GitHub/pyextract/ecfs",
  "connection_prefix": "oracle",
  "connection_type_select": "Load Balanced Connection"
}    
    '''
    data = {}
    for k in request.POST.keys():
        if (k not in [settings.OTP_CODE_VARIABLE]):
            data[k] = request.POST[k]
    fname = data.get('connection', None)
    dirname = data.get('ecfdirname', None)
    __is__ = False
    if (dirname):
        dirname = dirname.replace('/', os.sep)
        __is__ = os.path.exists(dirname) and os.path.isdir(dirname)
        
    d = {}
    if (__is__):
        fname = os.sep.join([dirname, fname + '.connection'])
        
        d['payload'] = {}
        try:
            with open(fname, 'r') as infile:
                data = json.load(infile)
                d['payload'] = data
        except:
            __is__ = False

    d['status'] = 'OK' if (__is__) else 'NOT_OK'
    d['fname'] = fname
    try:
        d['json_status'] = {}
    except Exception as ex:
        d['exception'] = str(ex)
    response = json.dumps(d)
    return HttpResponse(content=response, content_type="application/json")
