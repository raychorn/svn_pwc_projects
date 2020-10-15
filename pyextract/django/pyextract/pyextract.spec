# -*- mode: python -*-

block_cipher = None


a = Analysis(['manage.py'],
             pathex=['Z:\\Documents\\GitHub\\pyextract\\django\\pyextract'],
             binaries=[],
             datas=[],
             hiddenimports=[
				'django.contrib.messages.apps', 
				'django.contrib.staticfiles.apps',
				'django.contrib.sessions.models',
				'django.contrib.sessions.apps',
				'django.contrib.messages.middleware',
				'pyextract.wsgi.application',
				'django.contrib.sessions.middleware',
				'django.contrib.sessions.serializers'
				],
             hookspath=[],
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             cipher=block_cipher)
pyz = PYZ(a.pure, a.zipped_data,
             cipher=block_cipher)
exe = EXE(pyz,
          a.scripts,
          exclude_binaries=True,
          name='pyextract',
          debug=False,
          strip=False,
          upx=True,
          console=True )
coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               name='pyextract')
