import requests
import traceback
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

## DAW API
CONFIG_DICT = {}
CONFIG_DICT['DAW_DETAILS'] = (  # order matters!
    # ('prod-daw-api.azurewebsites.net',  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImFkYXB0IiwibmJmIjoxNTAwMDMyMzczLCJleHAiOjE1MTQ0MTkxNzMsImlhdCI6MTUwMDAzMjM3MywiaXNzIjoiREFXLUFQSSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NDAwMCJ9.Tbfa9tngnIEM3j6nWWOBTsO2g0HvVGnQvWGZvnGsMl0'),
    ('prod1-daw-api.azurewebsites.net', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImFkYXB0IiwibmJmIjoxNTA4MjY2MTA2LCJleHAiOjE1MzAzMTY3NjYsImlhdCI6MTUwODI2NjEwNiwiaXNzIjoiREFXLUFQSSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NDAwMCJ9.JfuoufNr2XfIE-V9C5BXGWBvFNRkTsdKx0Tj3taCez8'),
    ('prod2-daw-api.azurewebsites.net', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImFkYXB0IiwibmJmIjoxNTA4MjY2MzQ1LCJleHAiOjE1MzAzMTY3NjUsImlhdCI6MTUwODI2NjM0NSwiaXNzIjoiREFXLUFQSSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NDAwMCJ9.__NcbFBP6-SkHlKJKU5anpWMsBLjik3SsPB_iM36WsQ'),
    ('prod3-daw-api.azurewebsites.net', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImFkYXB0IiwibmJmIjoxNTA4MjY2NDEyLCJleHAiOjE1MzAzMTY3NzIsImlhdCI6MTUwODI2NjQxMiwiaXNzIjoiREFXLUFQSSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NDAwMCJ9.aAXLRqnVRFuWr5XbLt_mcvktF82reoRm-QxhxzmfE7I'),
    ('prod4-daw-api.azurewebsites.net', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImFkYXB0IiwibmJmIjoxNTA4MjY2NDczLCJleHAiOjE1MzAzMTY3NzMsImlhdCI6MTUwODI2NjQ3MywiaXNzIjoiREFXLUFQSSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NDAwMCJ9.klFojTSk39_TcG-XGwhBYGiSrHf51SbcFwbJ5pMbwoo'),
    ('prod5-daw-api.azurewebsites.net', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1bmlxdWVfbmFtZSI6ImFkYXB0IiwibmJmIjoxNTA4MjY2NjIzLCJleHAiOjE1MzAzMTY3NDMsImlhdCI6MTUwODI2NjYyMywiaXNzIjoiREFXLUFQSSIsImF1ZCI6Imh0dHA6Ly9sb2NhbGhvc3Q6NDAwMCJ9.flTolD8g5O5_2ymPgpX1K3BibGzO1ll7Zu6aixnqOv8')
)

CODE_STATUS_MAP = {
    "100": "Requested",
    "200": "Building",
    "400": "Ready",
    "410": "Starting",
    "500": "Stopped",
    "510": "Stopping",
    "600": "Destroyed",
    "610": "Destroying",
    "700": "RollbackStartup",
    "710": "RollbackShutdown",
    "900": "Failed",
}


def lookup(adapt_id, logger=None):

    vm_status = None
    vm_ipaddr = None
    headers = {"Accept": "application/json"}
    for domain, auth_header in CONFIG_DICT['DAW_DETAILS']:

        headers['Authorization'] = auth_header
        url = "https://%s/api/projectUpdate/%s" % (domain, adapt_id)
        response = requests.get(url, headers=headers, verify=False)
        response = response

        # Did we get back a successful response?
        if response.status_code == 200:
            if "Could not find Project with external id" in response.content.decode():
                print("200 Adapt ID not found at {}".format(domain))

            else:
                print("200 Adapt ID successfully identified at {}".format(domain))

                try:
                    rjson = response.json()
                    for vm in rjson["virtualMachines"]:
                        vm_status = CODE_STATUS_MAP.get(str(vm['status']))
                        vm_ipaddr = vm["ipAddress"]
                        return vm_status, vm_ipaddr

                except:
                    print("200 Failed to acquire VM Status/IP Address with Error: {}".format(traceback.format_exc()))

        # 404
        elif response.status_code == 404:
            print("404 URL Not Found: {}".format(url))

        # Other
        else:
            print("{} Error: {}".format(response.status_code, response.text))

    return vm_status, vm_ipaddr


if __name__== "__main__":

    adapt_id = 1080835   # My Test
    # adapt_id = 1087120
    status, ip = lookup(adapt_id)
    print("{}: {}".format(ip, status))
