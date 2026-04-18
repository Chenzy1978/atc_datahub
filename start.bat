@echo off
cd /d "%~dp0"
title ATC Data Hub
py -3 -m atc_data_hub run
pause

