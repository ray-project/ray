import os

import pytest

import ray
from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa

# Real HL7 messages from https://docs.webchartnow.com/functions/system-administration/interfaces/sample-hl7-messages/
# Note: HL7 messages use carriage return (\r) as segment separator, but Python strings
# use newline (\n). We'll write them with \r when creating test files.
# ADT^A08 - Patient Update
ADT_A08_MESSAGE = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20110614075841||ADT^A08|1407511|P|2.3||||||\rEVN|A08|20110614075841||\rPID|1||123456||MOUSE^MICKEY^S||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830|||||||||||||||||||\rPV1|1|O|8 ST M/O TEAM CARE E^8413/14^8413^6|||||2782^Physician^Samer|||||||||2782^Physician^Samer|||||||||||||||||||||||||||20110614"

# ORU^R01 - Lab Results
ORU_R01_MESSAGE = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20150325170228||ORU^R01|89799101|P|2.3||||||\rPID|1||12345|12345^^^MIE&1.2.840.114398.1.100&ISO^MR||MOUSE^MICKEY^S||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830|||||||||||||||||||\rIN1|1||1|ABC Insurance Medicaid|P O Box 12345^^Atlanta^GA^30348|Claims^Florida |(555)555-1234^^^^^^|G1234|||||||G|Mouse^Mickey|SELF|19281118|123 Main St.^^Lake Buena Vista^FL^32830|Y||||||||||||P||||ZYX1234589-1|||||||M||||M||\rORC|NW|23|||Pending||^^^^^0||20150325170228|26^David^Dave||8^Selenium^Selenium|^^^^OFFICE^^^^^Office|^^^[email protected]|||||||||||\rOBR|1|23||123^CREATININE|0|||||||||||8^Selenium^Selenium||||||||||||||||||||||||||||||||\rDG1|1|ICD|B60.0^BABESIOSIS^I10|BABESIOSIS||||||||||||\rOBR|2|23||80061^LIPID PROFILE|0|||||||||||8^Selenium^Selenium||||||||||||||||||||||||||||||||\rDG1|1|ICD|B60.0^BABESIOSIS^I10|BABESIOSIS||||||||||||"

# MDM^T02 - Document (Plain Text)
MDM_T02_PLAINTEXT = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20141003102429||MDM^T02^MDM_T02|89739|P|2.3|||||||||\rPID|1||10046||Testpatient^Tester||20030303000000||||||||||||||||||||||||||||\rTXA|1|TYPE|FT|20141002162958||20141002162958|20141002162958|20141002162958|4119531^Physician^Dr||4119531^Physician^Dr|1111|||||LA|||||4119531^Physician^Dr^^^^^^^^1^^^^20141002162958|\rOBX|1|TX|4|1|I am an example free text Document|||||||||||||||||||"

# ACK - Success Acknowledgement
ACK_SUCCESS = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20110614075841||ACK|1407511|P|2.3||||||\rMSA|AA|1407511|Success||"

# ACK - Error Acknowledgement
ACK_ERROR = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|20110614075841||ACK|1407511|P|2.3||||||\rMSA|AE|1407511|Error processing record!||"

# VXU^V04 - Immunization
VXU_V04 = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|201305171259|12|VXU^V04|2244455|P|2.3||||||\rPID|1||123456||DUCK^DAISY^L||19690912|F|||123 NORTHWOOD ST APT 9^^NEW CITY^NC^27262-9944|||||||||||||||||||\rORC|OK|664443333^EEE|33994499||||^^^20220301||20220301101531|DAVE^DAVID^DAVE^D||444999^DAVID JR^JAMES^DAVID^^^^^LAB&PROVID&ISO^L^^^PROVID^FACILITY_CODE&1.2.888.444999.1.13.308.2.7.2.696969&ISO|1021209999^^^10299^^^^^WD999 09 LABORATORY NAME|^^^^^333^8022999||||CCC528Y73^CCC-528Y73||||||\rRXA|0|999|20220301|20220301|217^PFIZER 12 YEARS \\T\\ UP SARS-COV-2 VACCINE^LIM_CVX|0.3|ML||00^New immunization record^NIP001|459920^DUCK^DAISY^L^^^^^LAB&PROVID&ISO^L^^^PROVID^FACILITY_CODE&1.2.888.444999.1.13.308.2.7.2.696969&ISO|1021209999^^^10299^^^^^WD999 09 LABORATORY NAME||||FK9999|20220531|PPR|||CP|A|20220301101531\rRXR|IM^Intramuscular^HL70162|LD^Left Deltoid^HL70163|||"


def test_read_basic_hl7_file(ray_start_regular_shared, tmp_path):
    """Test reading a basic HL7 file with parsed messages."""
    path = os.path.join(tmp_path, "sample.hl7")
    with open(path, "wb") as f:
        f.write(ADT_A08_MESSAGE.encode("utf-8"))

    ds = ray.data.read_hl7(path)

    rows = ds.take_all()
    assert len(rows) == 1
    assert "message_id" in rows[0]
    assert "message_type" in rows[0]
    assert "segments" in rows[0]
    assert rows[0]["message_type"] == "ADT^A08"
    assert rows[0]["message_id"] == "1407511"


def test_read_multiple_messages(ray_start_regular_shared, tmp_path):
    """Test reading a file with multiple HL7 messages."""
    path = os.path.join(tmp_path, "multi.hl7")
    # Use double carriage return to separate messages
    content = ADT_A08_MESSAGE + "\r\r" + ORU_R01_MESSAGE
    with open(path, "wb") as f:
        f.write(content.encode("utf-8"))

    ds = ray.data.read_hl7(path)

    rows = ds.take_all()
    assert len(rows) == 2
    assert rows[0]["message_type"] == "ADT^A08"
    assert rows[1]["message_type"] == "ORU^R01"


def test_read_raw_messages(ray_start_regular_shared, tmp_path):
    """Test reading HL7 files without parsing."""
    path = os.path.join(tmp_path, "raw.hl7")
    with open(path, "wb") as f:
        f.write(ADT_A08_MESSAGE.encode("utf-8"))

    ds = ray.data.read_hl7(path, parse_messages=False)

    rows = ds.take_all()
    assert len(rows) == 1
    assert "message" in rows[0]
    assert isinstance(rows[0]["message"], str)
    assert "MSH" in rows[0]["message"]


def test_read_empty_hl7_file(ray_start_regular_shared, tmp_path):
    """Test reading an empty HL7 file."""
    path = os.path.join(tmp_path, "empty.hl7")
    # Create empty file
    open(path, "wb").close()

    ds = ray.data.read_hl7(path)

    assert ds.count() == 0


def test_read_hl7_with_different_encodings(ray_start_regular_shared, tmp_path):
    """Test reading HL7 files with different encodings."""
    path = os.path.join(tmp_path, "latin1.hl7")
    with open(path, "wb") as f:
        f.write(ADT_A08_MESSAGE.encode("latin-1"))

    ds = ray.data.read_hl7(path, encoding="latin-1")

    rows = ds.take_all()
    assert len(rows) == 1


def test_read_hl7_with_custom_separators(ray_start_regular_shared, tmp_path):
    """Test reading HL7 files with custom segment separators."""
    path = os.path.join(tmp_path, "custom.hl7")
    # Use newline instead of carriage return
    content = ADT_A08_MESSAGE.replace("\r", "\n")
    with open(path, "wb") as f:
        f.write(content.encode("utf-8"))

    ds = ray.data.read_hl7(path, segment_separator="\n")

    rows = ds.take_all()
    assert len(rows) == 1


def test_read_hl7_ack_messages(ray_start_regular_shared, tmp_path):
    """Test reading ACK (acknowledgement) messages."""
    path = os.path.join(tmp_path, "ack.hl7")
    content = ACK_SUCCESS + "\r\r" + ACK_ERROR
    with open(path, "wb") as f:
        f.write(content.encode("utf-8"))

    ds = ray.data.read_hl7(path)

    rows = ds.take_all()
    assert len(rows) == 2
    # Both should have message_type from MSH-9
    assert all("message_type" in row for row in rows)


def test_read_hl7_lab_results(ray_start_regular_shared, tmp_path):
    """Test reading ORU^R01 lab result messages."""
    path = os.path.join(tmp_path, "lab.hl7")
    with open(path, "wb") as f:
        f.write(ORU_R01_MESSAGE.encode("utf-8"))

    ds = ray.data.read_hl7(path)

    rows = ds.take_all()
    assert len(rows) == 1
    assert rows[0]["message_type"] == "ORU^R01"
    # Should have multiple segments (MSH, PID, IN1, ORC, OBR, DG1)
    assert len(rows[0]["segments"]) > 3


def test_read_hl7_immunization(ray_start_regular_shared, tmp_path):
    """Test reading VXU^V04 immunization messages."""
    path = os.path.join(tmp_path, "immunization.hl7")
    with open(path, "wb") as f:
        f.write(VXU_V04.encode("utf-8"))

    ds = ray.data.read_hl7(path)

    rows = ds.take_all()
    assert len(rows) == 1
    assert rows[0]["message_type"] == "VXU^V04"


def test_read_hl7_document(ray_start_regular_shared, tmp_path):
    """Test reading MDM^T02 document messages."""
    path = os.path.join(tmp_path, "document.hl7")
    with open(path, "wb") as f:
        f.write(MDM_T02_PLAINTEXT.encode("utf-8"))

    ds = ray.data.read_hl7(path)

    rows = ds.take_all()
    assert len(rows) == 1
    assert rows[0]["message_type"] == "MDM^T02^MDM_T02"


def test_read_hl7_multiple_files(ray_start_regular_shared, tmp_path):
    """Test reading multiple HL7 files from a directory."""
    dir_path = os.path.join(tmp_path, "hl7_dir")
    os.mkdir(dir_path)

    with open(os.path.join(dir_path, "file1.hl7"), "wb") as f:
        f.write(ADT_A08_MESSAGE.encode("utf-8"))
    with open(os.path.join(dir_path, "file2.hl7"), "wb") as f:
        f.write(ORU_R01_MESSAGE.encode("utf-8"))

    ds = ray.data.read_hl7(dir_path)

    rows = ds.take_all()
    assert len(rows) == 2


def test_read_hl7_file_extensions(ray_start_regular_shared, tmp_path):
    """Test that HL7 datasource recognizes different file extensions."""
    dir_path = os.path.join(tmp_path, "extensions")
    os.mkdir(dir_path)

    with open(os.path.join(dir_path, "file1.hl7"), "wb") as f:
        f.write(ADT_A08_MESSAGE.encode("utf-8"))
    with open(os.path.join(dir_path, "file2.hl7v2"), "wb") as f:
        f.write(ORU_R01_MESSAGE.encode("utf-8"))
    with open(os.path.join(dir_path, "file3.msg"), "wb") as f:
        f.write(ACK_SUCCESS.encode("utf-8"))
    with open(os.path.join(dir_path, "file4.txt"), "wb") as f:
        f.write(b"not hl7")

    ds = ray.data.read_hl7(dir_path)

    rows = ds.take_all()
    # Should only read .hl7, .hl7v2, .msg files
    assert len(rows) == 3


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
