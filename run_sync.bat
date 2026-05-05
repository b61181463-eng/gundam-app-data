@echo off
cd /d %~dp0
python sync_to_firestore.py
pause