# -*- mode: python ; coding: utf-8 -*-

import os

from PyInstaller.building.build_main import Analysis, PYZ, EXE, COLLECT

# Derive paths from the spec location so the build works on any machine.
# SPECPATH is injected by PyInstaller; fall back to this file's directory.
_HERE = os.path.abspath(globals().get('SPECPATH') or os.path.dirname(os.path.abspath(__file__)))
# The repo dir holds the entrypoint; its parent is what makes `mq.*` importable.
_PATHEX = [_HERE, os.path.dirname(_HERE)]

# Ship certifi's CA bundle so the frozen binary has a usable trust store.
try:
    import certifi

    certifi_datas = [(certifi.where(), 'certifi')]
except Exception:
    certifi_datas = []

a = Analysis(
    ['cli.py'],
    pathex=_PATHEX,
    binaries=[],
    datas=certifi_datas,
    hiddenimports=[
        'certifi',
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
        'mq.commands',
        'mq.commands.info_queue',
        'mq.commands.inventory_cmd',
        'mq.services.discovery',
        'mq.inventory',
        'mq.errors',
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
