#!/bin/sh
trap "echo SIGINT; exit" SIGINT
trap "echo SIGTERM; exit" SIGTERM
echo Starting script
sleep 10