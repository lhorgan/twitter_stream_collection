#!/bin/bash

# https://superuser.com/questions/1218575/best-way-to-auto-restart-mono-process-after-crash
while (true) do
    echo "Here we go! You have a life vest, right?"
    python3 stream.py
    exitcode=$?
    echo "Stream crashed with exit code $exitcode"
done
