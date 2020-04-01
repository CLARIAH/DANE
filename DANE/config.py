# Copyright 2020-present, Netherlands Institute for Sound and Vision (Nanne van Noord)
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################

from yacs.config import CfgNode as CN
import os, sys

__all__ = ["cfg"]

cfg = CN(new_allowed=True)

cfg.DANE = CN(new_allowed=True)
cfg.DANE.HOST = '0.0.0.0' # host we listen on
cfg.DANE.PORT = 5500
if 'DANE_HOME' in os.environ.keys():
    cfg.DANE.HOME_DIR = os.path.join(os.environ['DANE_HOME'], '')
else:
    cfg.DANE.HOME_DIR = os.path.join(os.environ['HOME'], ".DANE", '')

# cwd might not be same as dir where the file being called is in, resolve this
xdir, _ = os.path.split(os.path.join(os.getcwd(), sys.argv[0]))

cfg.DANE.LOCAL_DIR = os.path.join(xdir, '')
cfg.DANE.API_URL = 'http://localhost:5500/DANE/' # URL the api can be reached at
cfg.DANE.MANAGE_URL = 'http://localhost:5500/manage/'

cfg.RABBITMQ = CN()
cfg.RABBITMQ.HOST = 'localhost'
cfg.RABBITMQ.PORT = 5672
cfg.RABBITMQ.EXCHANGE = 'DANE-exchange'
cfg.RABBITMQ.RESPONSE_QUEUE = 'DANE-response-queue'
cfg.RABBITMQ.USER = 'guest'
cfg.RABBITMQ.PASSWORD = 'guest'

cfg.CUDA = CN(new_allowed=True)
cfg.CUDA.VISIBLE_DEVICES = '1'

# Does the home dir have a config with additional param?
# Add them. Or override defaults defined here
if os.path.exists(os.path.join(cfg.DANE.HOME_DIR, "config.yml")):
    cfg.merge_from_file(os.path.join(cfg.DANE.HOME_DIR, "config.yml"))

# Does the local dir have a base_config with additional param?
if os.path.exists(os.path.join(cfg.DANE.LOCAL_DIR, "base_config.yml")):
    cfg.merge_from_file(os.path.join(cfg.DANE.LOCAL_DIR, "base_config.yml"))

# Does the local dir have a config with specific param?
if os.path.exists(os.path.join(cfg.DANE.LOCAL_DIR, "config.yml")):
    cfg.merge_from_file(os.path.join(cfg.DANE.LOCAL_DIR, "config.yml"))

# make immutable
cfg.freeze()
