#! /bin/bash -x
BUILDINFO=`git describe --long|sed -e's/Version-//' | sed -e"s/-/./;s/-HEAD.*//"`
COMMIT=`echo ${BUILDINFO}|sed -e"s/.*\-//"`
VERSION_STRING=`echo ${BUILDINFO}|sed -e"s/\-.*$//"`
RELEASE=${VERSION_STRING}-${CI_COMMIT_REF_NAME}-${COMMIT}
MAJOR=$((`echo ${VERSION_STRING}|cut -d. -f1`+0))
MINOR=$((`echo ${VERSION_STRING}|cut -d. -f2`+0))
BUILD=$((`echo ${VERSION_STRING}|cut -d. -f3`+0))

cat > ${CI_PROJECT_NAME}_version.env << EOF
export P28_BUILD_DATE=$(date +%Y-%m-%d)
export P28_BUILD_TIME=$(date +%H:%M:%S)
export P28_BUILD_USER=${USER}
export P28_VERSION_STRING="${VERSION_STRING}"
export P28_RELEASE="${RELEASE}"
export P28_MAJOR_VERSION="${MAJOR}"
export P28_MINOR_VERSION="${MINOR}"
export P28_BUILD_NUMBER="${BUILD}"
export P28_BRANCH="${CI_COMMIT_BRANCH}"
export P28_VERSION_NUMBER="`printf '0x%02d%02d%02d' $MAJOR $MINOR $BUILD`"
EOF
