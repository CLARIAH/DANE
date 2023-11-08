from dataclasses import dataclass, field
import json
import logging
from time import time
from typing import Any, Dict, List, Optional
from dane.config import cfg


logger = logging.getLogger("DANE")
SOFTWARE_PROVENANCE_FILE = "/software_provenance.txt"
PROVENANCE_FILE = "provenance.json"


@dataclass
class Provenance:
    # these parameters should be known before processing starts
    activity_name: str
    activity_description: str
    input_data: Dict[str, Any]
    start_time_unix: float
    parameters: Dict[str, Any] = field(default_factory=dict)
    software_version: Dict[str, Any] = field(default_factory=dict)

    # empty when process just started; available when the process is done
    output_data: Optional[Dict[str, Any]] = None
    processing_time_ms: Optional[float] = -1
    steps: Optional[list["Provenance"]] = field(default_factory=list["Provenance"])

    def to_json(self):
        processing_steps = self.steps if self.steps else []
        return {
            "activity_name": self.activity_name,
            "activity_description": self.activity_description,
            "processing_time_ms": self.processing_time_ms,
            "start_time_unix": self.start_time_unix,
            "parameters": self.parameters,  # .to_json
            "software_version": self.software_version,  # .to_json
            "input_data": self.input_data,  # .to_json
            "output_data": self.output_data,  # .to_json
            "steps": [step.to_json() for step in processing_steps],
        }


def generate_initial_provenance(
    name: str,
    description: str,
    input_data: Dict[str, Any],
    parameters: Dict[str, Any] = {},
    software_version: Dict[str, Any] = {},
    start_time: float = time(),
):
    return Provenance(
        activity_name="VisXP prep",
        activity_description=(
            "Detect shots and keyframes, "
            "extract keyframes and corresponding audio spectograms"
        ),
        input_data=input_data,
        parameters=cfg.VISXP_PREP,
        software_version=software_version,
        start_time_unix=start_time,
    )


# Generates a the main Provenance object, which will embed/include the provided provenance_chain
def stop_timer_and_persist_provenance_chain(
    provenance: Provenance,
    output_data: Dict[str, Any],
    provenance_chain: List[Provenance],
    provenance_file_path: str,  # where to write provenance.json
) -> Provenance:
    provenance.output_data = output_data
    provenance.processing_time_ms = time() - provenance.start_time_unix
    provenance.steps = provenance_chain

    fdata = provenance.to_json()
    logger.info("Going to write the following to disk:")
    logger.info(fdata)
    with open(provenance_file_path, "w", encoding="utf-8") as f:
        json.dump(fdata, f, ensure_ascii=False, indent=4)
        logger.info(f"Wrote provenance info to file: {provenance_file_path}")
    return provenance


# NOTE: software_provenance.txt is created while building the container image (see Dockerfile)
def obtain_software_versions(software_names):
    if isinstance(software_names, str):  # wrap a single software name in a list
        software_names = [software_names]
    try:
        with open(SOFTWARE_PROVENANCE_FILE) as f:
            urls = (
                {}
            )  # for some reason I couldnt manage a working comprehension for the below - SV
            for line in f.readlines():
                name, url = line.split(";")
                if name.strip() in software_names:
                    urls[name.strip()] = url.strip()
            assert len(urls) == len(software_names)
            return urls
    except FileNotFoundError:
        logger.info(
            f"Could not read {software_names} version"
            f"from file {SOFTWARE_PROVENANCE_FILE}: file does not exist"
        )
    except ValueError as e:
        logger.info(
            f"Could not parse {software_names} version"
            f"from file {SOFTWARE_PROVENANCE_FILE}. {e}"
        )
    except AssertionError:
        logger.info(
            f"Could not find {software_names} version"
            f"in file {SOFTWARE_PROVENANCE_FILE}"
        )
