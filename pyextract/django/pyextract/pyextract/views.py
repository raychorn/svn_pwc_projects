from django.template.loader import get_template
from django.template import Context
from django.http import HttpResponse, HttpResponseNotFound, HttpResponseNotAllowed, HttpResponseRedirect
from django.conf import settings

import os
import psutil
import uuid

import time

from pyextract.rest import render_drives
from pyextract.rest import render_directory_contents

from pyextract.rest import rest_token
from pyextract.rest import SameDomainRequired

from pyextract.rest import LoginRequired

from pyextract.rest import is_token_valid

from pyextract.rest import set_in_session



def render_path_as_anchors(fp, URI='/choose_ecf_file'):
    items = []
    __toks__ = []
    toks = fp.split(sep=os.sep)
    i = 0
    for t in toks[0:-1]:
        if (len(t) == 0):
            i += 1
            continue
        elif (i == 2) and (t == 'Mac'):
            i += 1
            continue
        else:
            break
    prefix = toks[0:i]
    for t in prefix:
        items.append(t)
    for t in toks[i:-1]:
        __toks__.append(t)
        partial = os.sep.join(prefix+__toks__)
        if (t not in [os.sep]) and (len(t) > 0):
            item = '<a class="ui-button ui-widget ui-corner-all" href="%s?top=%s">%s</a>' % (URI, partial, t)
            items.append(item)
    items.append(toks[-1])
    return items


def _partial_file_chooser(top=os.path.abspath(os.path.curdir), URI='/choose_ecf_file', cachebuster=str(uuid.uuid4())):
    tplate = get_template('_partial_file_chooser.html')
    fpath = top
    fpath_content = os.sep.join(render_path_as_anchors(fpath, URI=URI))
    c = {}
    c['the_drives'] = render_drives(cwd=fpath, URI=URI, cachebuster=cachebuster)
    c['the_directory'] = fpath_content
    c['the_contents'] = render_directory_contents(fpath, URI=URI, cachebuster=cachebuster)
    response = tplate.render(c)
    return response


@SameDomainRequired()
def choose_ecf_file(request):
    top = request.GET.get('top', default=os.path.abspath(os.path.curdir))
    tplate = get_template('choose_ecf_file.html')
    fpath = top
    fpath_content = os.sep.join(render_path_as_anchors(fpath))
    c = {}
    c['the_drives'] = render_drives(cwd=fpath)
    c['the_directory'] = fpath_content
    c['the_contents'] = render_directory_contents(fpath)
    c['isnotpartial'] = True
    response = tplate.render(c)
    return HttpResponse(content=response)


@SameDomainRequired()
def configs(request):
    tplate = get_template('configs.html')
    response = tplate.render({})
    return HttpResponse(content=response)


@SameDomainRequired()
def manage_saved_connections(request):
    tplate = get_template('manage_saved_connections.html')
    response = tplate.render({})
    return HttpResponse(content=response)


@SameDomainRequired()
def preview_ecf_file(request):
    ecf_file = request.GET.get('ecf_file', default=os.path.abspath(os.path.curdir))
    tplate = get_template('preview_ecf_file.html')
    c = {}
    c['ecf_file'] = ecf_file
    c['ecf_file_dirname'] = os.path.dirname(ecf_file)
    c['the_ecftext'] = '<br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/>'
    c['the_helptext'] = """Please browse and select a valid Extraction Configuration File (ECF) and review the configuration details for accuracy. By clicking Next, you confirm the details are correct. If there is an error, please contact your PwC Engagement Team contact for support."""
    response = tplate.render(c)
    return HttpResponse(content=response)


def pretty_key(key):
    response = ''
    for ch in key:
        if (str(ch).isupper()):
            ch = ' ' + ch
        response += ch
    return str(response).strip()


@SameDomainRequired()
def preview_ecf_file2(request):
    has_ecf_file = request.GET.get('ecf_file', default=None) is not None
    ecf_file = request.GET.get('ecf_file', default=os.path.abspath(os.path.curdir))
    top = request.GET.get('top', default=ecf_file)
    if (not has_ecf_file):
        top = top if (not os.path.isfile(top)) else os.path.dirname(top)
    if (top):
        top = top.replace('/', os.sep)
    tplate = get_template('preview_ecf_file2.html')
    c = {}
    c['ecf_file'] = top
    c['ecf_file_dirname'] = os.path.dirname(top)
    c['the_ecftext'] = '<br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/><br/>'
    c['the_helptext'] = """Please browse and select a valid Extraction Configuration File (ECF) and review the configuration details for accuracy. By clicking Next, you confirm the details are correct. If there is an error, please contact your PwC Engagement Team contact for support."""
    c['isfile'] = __isfile__ = os.path.isfile(top)
    c['isfile2'] = __isfile__
    c['isnotpartial'] = False
    if (not __isfile__):
        c['the_file_chooser'] = _partial_file_chooser(top=top, URI=request.path, cachebuster=request.GET.get('cb', str(uuid.uuid4())))
    else:
        from ecfreader import read_encrypted_json
        status = {}
        data = read_encrypted_json(ecf_file, status=status)
        previewable = {}
        invalid_keys = ['PublicKey', 'Username', 'ChannelLos', 'PublicKeyType', 'DataSourceId', 'DatabaseServerName', 'IsDatabaseNameRequired', 'Original Application Id', 'Request Id', 'File Upload Method', 'Current Application Id']
        invalid_keys = [str(''.join([ch for ch in k.split(' ') if (not str(ch).isspace())])).lower() for k in invalid_keys]
        for k in data.keys():
            if (not isinstance(data[k], list)) and (not isinstance(data[k], dict)):
                if (str(k).lower() not in invalid_keys):
                    previewable[k] = data[k]
        c['isfile'] = False
        c['the_file_chooser'] = '<TABLE>' + ''.join(['<tr><td width="40%%">%s</td><td>%s</td></tr>' % (pretty_key(k), previewable[k]) for k in sorted(previewable.keys())]) + '</TABLE>'

    c['top'] = top if (not __isfile__) else os.path.dirname(top)
    c['cancel_link'] = '/' if (not has_ecf_file) else '/preview_ecf_file2?top=' + top
    response = tplate.render(c)
    return HttpResponse(content=response)


@SameDomainRequired()
def handle_connection(request):
    ecf_file = request.GET.get('ecffile', default=os.path.abspath(os.path.curdir))
    from ecfreader import read_encrypted_json
    c = {}
    status = {}
    data = read_encrypted_json(ecf_file, status=status)
    datasource = data.get('DataSource', {})
    __is_oracle__ = False
    for k in datasource.keys():
        v = datasource[k]
        c['datasource_%s' % (k)] = v
        if (str(v).lower().find('oracle') > -1):
            __is_oracle__ = True
    c['isoracle'] = __is_oracle__

    c['csrf_token'] = rest_token()
    print('DEBUG.1: authcode = %s' % (c['csrf_token']))
    if (__is_oracle__):
        tplate = get_template('oracle_connection.html')

        default_connection_json_fname = os.sep.join([os.path.dirname(ecf_file), 'oracle_connection.json'])
        if (os.path.exists(default_connection_json_fname) and os.path.isfile(default_connection_json_fname)):
            import json
            with open(default_connection_json_fname, 'r') as ecf:
                rawdata = ecf.read()
        
            connection = {}
            try:
                connection = json.loads(rawdata)
            except Exception as ex:
                c['exception'] = str(ex)
                connection = {}

            for k in connection.keys():
                v = connection[k]
                c['connection_%s' % (k)] = v
    else:
        tplate = get_template('unknown_connection.html')

    c['ecfdirname'] = os.path.dirname(ecf_file).replace(os.sep, '/')
    c['ecf_file'] = ecf_file
    
    saved_top = os.path.dirname(default_connection_json_fname)
    saved_files = [f for f in os.listdir(saved_top) if (os.path.splitext(f)[-1] == '.connection')]
    saved_list = ['<option selected="selected">None</option>']
    for f in saved_files:
        saved_list.append('<option>' + os.path.splitext(f)[0] + '</option>')
    c['saved_connections_list'] = ''.join(saved_list)

    response = tplate.render(c)
    return HttpResponse(content=response)


@SameDomainRequired()
def get_login_token(request):
    tplate = get_template('get_login_token.html')
    c = {}
    c['login_token'] = rest_token()
    response = tplate.render(c)
    request.session['login_token_valid'] = False # force the logged-in state to be False whenever someone asks for a token.
    return HttpResponse(content=response)


#@SameDomainRequired()
@LoginRequired()
def main_view(request):
    tplate = get_template('main.html')
    c = {}
    response = tplate.render(c)
    return HttpResponse(content=response)


@SameDomainRequired()
def handle_user_login(request):
    login_token = request.POST.get('login_token', default='')
    login_token = str(login_token).strip()
    __is__ = is_token_valid(login_token)
    set_in_session(request, 'login_token_valid', value=__is__)
    print('login_token is %s (%s)' % (login_token, __is__))
    referer = request.META.get('HTTP_REFERER')
    http_host = request.META.get('HTTP_HOST')
    print('DEBUG.4: login_token_valid = %s for %s%s from %s' % (__is__, http_host, request.path, referer))
    return HttpResponseRedirect('/?cb=%s' % (request.POST.get('cb')) if (request.POST.get('cb', None) is not None) else '/')

