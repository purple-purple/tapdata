#!/bin/bash 
 
set -u

__bash_dir__="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# SKIP_SDC_SOURCE_TEST="${SKIP_SDC_SOURCE_TEST:-false}"
SKIP_BUILD_LIB="${SKIP_BUILD_LIB:-false}"
SKIP_RUN_SDC=${SKIP_RUN_SDC:-true}
DEV_MODE="${DEV_MODE:-false}"
DOWNLOAD_SDC="${DOWNLOAD_SDC:-false}"
BETA="${BETA:-false}"

# Get git tag build version
GIT_TAG=`git tag`
echo DEV_MODE:"${DEV_MODE}"
if [ -z "$GIT_TAG" -o "$BETA" == "true" ];then
    TAP_DATA_VERSION=tapdata-beta
else
    GIT_TAG_VERSION=`git describe --long HEAD`
    TAP_DATA_VERSION=tapdata-"${GIT_TAG_VERSION}"
fi
echo TAP_DATA_VERSION:${TAP_DATA_VERSION}
export TAP_DATA_VERSION

TAPDATA_FINAL_NAME=tapdata

PID=$(ps -ef|grep ./dist/target/"${TAPDATA_FINAL_NAME}"|grep -v grep|awk '{print $2}')

make_dist_dir() {
    if [ ! -d "${__bash_dir__}/dist/" ]; then
        mkdir "${__bash_dir__}/dist"
    fi
    if [ ! -d "${__bash_dir__}/dist/target" ]; then
        mkdir "${__bash_dir__}/dist/target"
    fi
    if [ ! -d "${__bash_dir__}/dist/target/${TAPDATA_FINAL_NAME}" ]; then
        mkdir "${__bash_dir__}/dist/target/${TAPDATA_FINAL_NAME}"
    fi
}

download_sdc() {
    echo "start to download sdc core"
    export SDC_VERSION="3.2.0.0-SNAPSHOT"
    cd "${__bash_dir__}/dist"
    if [ ! -f "${__bash_dir__}/dist/streamsets-datacollector-core-${SDC_VERSION}.tgz" ]; then
        curl -O http://nightly.streamsets.com.s3-us-west-2.amazonaws.com/datacollector/latest/tarball/streamsets-datacollector-core-${SDC_VERSION}.tgz
    fi
    rm -rf "${__bash_dir__}/dist/target/${TAPDATA_FINAL_NAME}"
    tar -xvf streamsets-datacollector-core-${SDC_VERSION}.tgz  &> /dev/null
    mv streamsets-datacollector-${SDC_VERSION} "${__bash_dir__}/dist/target/${TAPDATA_FINAL_NAME}"
    rm  -rf "target/${TAPDATA_FINAL_NAME}/sdc-static-web"
    cd "${__bash_dir__}"
}

install_ui_lib() {
    cd "${__bash_dir__}/datacollector-ui"
    if hash yarn 2>/dev/null; then
        yarn install
        yarn global add bower
        yarn global add grunt-cli
        bower install --allow-root
    else
        echo 'please install yarn for build environment.'
        exit 1;
    fi

    
}
build_ui() {
    echo "start to build html"
    cd "${__bash_dir__}/datacollector-ui"
  
    grunt build --force
}

watch_ui() {
    echo "start to grunt watch"
    cd "${__bash_dir__}/datacollector-ui"
    grunt watch --force
}

run_sdc() {
    #echo "start to check sdc download"
    #download_sdc

    echo "start to run sdc"
    cd "${__bash_dir__}"/dist/target/"${TAPDATA_FINAL_NAME}"
    
    export SDC_FILE_LIMIT=1024
    # dist/sdc/bin/streamsets dc
    # BUILD_ID=dontKillMe  nohup dist/sdc/bin/streamsets dc &
    BUILD_ID=dontKillMe nohup ./bin/tapdata dc>nohup.out 2>&1 &
}

main () {
    
    make_dist_dir 
    
    # if [ "$SKIP_SDC_SOURCE_TEST" != "true" ]; then
    #     download_sdc
    # fi

    if [ "$SKIP_RUN_SDC" = "false" ]; then
        #kill $(lsof -t -i:18630)
        if [ "$PID" != "" ]; then
            echo KILL "${TAPDATA_FINAL_NAME}":{$PID}
            kill -9 $PID
        else
            echo "${TAPDATA_FINAL_NAME}" IS NOT RUNNING
        fi

        if [ "$DOWNLOAD_SDC" = "true" ]; then
            download_sdc
        fi
        echo 'starting sdc'
        run_sdc
    else
        echo 'not starting sdc'
    fi


    if [ "$SKIP_BUILD_LIB" != "true" ]; then
       install_ui_lib
    fi

    if [ "$DEV_MODE" = "true" ]; then
        watch_ui
    else 
        build_ui
        echo "Done: built dist html files in dist/target/${TAPDATA_FINAL_NAME}"
        sed -i.bak "s#TAPDATA_VERSION_INFO#$TAP_DATA_VERSION#" "${__bash_dir__}/dist/target/${TAPDATA_FINAL_NAME}/sdc-static-web/templates-app.js"
    fi
}
rm -rf designer
git submodule update
main "$@"

