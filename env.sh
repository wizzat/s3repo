export VIMSERVER=DEV
export S3CACHE_CONFIG=~/work/s3cache/.test_config

function s3cache_env {
    export PS1="S3CACHE \W$ "
    export PYTHONPATH=`find_up_dir env.sh ~/work/s3cache`
}
