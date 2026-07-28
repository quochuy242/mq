# -*- mode: python ; coding: utf-8 -*-

from PyInstaller.building.build_main import Analysis, PYZ, EXE, COLLECT

a = Analysis(
    ['cli.py'],
    pathex=['/home/quochuy242/project/mq', '/home/quochuy242/project'],
    binaries=[],
    datas=[],
    hiddenimports=[
        'pika',
        'pika.adapters.blocking_connection',
        'pika.adapters',
        'pika.connection',
        'pika.channel',
        'pika.exceptions',
        'pika.spec',
        'pika.credentials',
        'pika.heartbeat',
        'typer',
        'orjson',
        'rich',
        'rich.markup',
        'rich.table',
        'rich.syntax',
        'rich.console',
        'shellingham',
        'yaml',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter',
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'PIL',
        'PyQt5',
        'PySide6',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='mq',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)
