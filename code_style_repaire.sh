#!/bin/bash
set -e
current_dir=`cd $(dirname $0);pwd`
cd $current_dir

function check_code_style() {
    # check yapf installation
    # fails when no yapf installed
    install=`pip3 install yapf==0.31.0 -i https://pypi.tuna.tsinghua.edu.cn/simple`

    which yapf >/dev/null
    local invalid_code=`yapf --style .yapf-code-style --diff --recursive ./ --exclude venv`
    if [ -n "$invalid_code" ]
    then
        echo "${invalid_code}"
        echo "code style not match, use 'yapf --style .yapf-code-style --in-place --recursive --exclude venv ./' to format your code"
        exit 10
    else
        echo
        echo
        echo "`date +'%F %T'` code style check pass"
        echo
        echo
    fi
}


check_code_style

