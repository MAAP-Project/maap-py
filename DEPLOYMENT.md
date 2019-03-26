```bash
git clone -b abarciauskas-bgse_add-browse git@github.com:developmentseed/maap-py.git maap_base/maap-py

export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
docker build \
  --buid-arg AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  --build-arg AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
  -t maap-py:latest -f Dockerfile.maap_py .

docker run -it -p 8888:8888 maap-py:latest /bin/bash -c "/opt/conda/bin/jupyter notebook --notebook-dir=/opt/notebooks --ip='*' --port=8888 --no-browser"
```