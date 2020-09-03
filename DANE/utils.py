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

import json
import sys
import subprocess
import DANE

def get_git_revision():
    return subprocess.check_output(['git', 
        'rev-parse', '--short', 'HEAD']).decode('ascii').strip()

def get_git_remote():
    return subprocess.check_output(['git', 
        'config', '--get', 'remote.origin.url']).decode('ascii').strip()

def cwd_is_git():
    try:
        subprocess.check_output(['git', 'branch'], stderr=subprocess.DEVNULL)
        return True
    except:
        return False
