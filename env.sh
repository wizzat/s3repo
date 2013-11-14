export VIMSERVER=DEV

function s3cache_env {
    export PS1="S3REPO \W$ "
    export PYTHONPATH=`find_up_dir env.sh ~/work/s3repo`
}
