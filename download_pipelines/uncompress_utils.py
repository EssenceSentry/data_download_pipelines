import gzip
import os
import tarfile
import tempfile
from typing import Optional
from zipfile import ZipFile

from download_pipelines.logging_utils import set_logger

logger = set_logger(__name__)


def untar(filename: str):
    logger.debug("Uncompressing tar file: %s" % filename)
    with tarfile.open(filename) as tar:
        temp_dir = tempfile.mkdtemp()
        contents = {
            os.path.join(temp_dir, member.name): member
            for member in tar.getmembers()
        }
        def is_within_directory(directory, target):
            
            abs_directory = os.path.abspath(directory)
            abs_target = os.path.abspath(target)
        
            prefix = os.path.commonprefix([abs_directory, abs_target])
            
            return prefix == abs_directory
        
        def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
        
            for member in tar.getmembers():
                member_path = os.path.join(path, member.name)
                if not is_within_directory(path, member_path):
                    raise Exception("Attempted Path Traversal in Tar File")
        
            tar.extractall(path, members, numeric_owner=numeric_owner) 
            
        
        safe_extract(tar, temp_dir, list(contents.values()))
    contents_filenames = list(contents.keys())
    logger.info("Uncompressed tar file %s. Contents:\n\t%s" %
                (filename, "\t".join(contents_filenames)))
    return contents_filenames


def ungzip(filename: str):
    logger.debug("Uncompressing gz file: %s" % filename)
    with gzip.open(filename) as f:
        temp = tempfile.NamedTemporaryFile(mode="wb+", delete=False)
        name = os.path.join(os.path.dirname(temp.name),
                            os.path.basename(filename.split(".")[0]))
        with open(name, "wb") as g:
            g.write(f.read())
    logger.info("Uncompressed gz file %s to %s" % (filename, name))
    return [name]


def unzip(filename: str, password: Optional[str] = None):
    message1 = "Uncompressing zip file %s" % filename
    message2 = " with password: %s" % password if password else ""
    logger.debug(message1 + message2)
    with ZipFile(filename) as _zip:
        temp_dir = tempfile.mkdtemp()
        if password:
            _zip.setpassword(password.encode())
        contents = {
            os.path.join(temp_dir, member.filename): member
            for member in _zip.infolist()
        }
        _zip.extractall(temp_dir, [v.filename for v in contents.values()])
    contents_filenames = list(contents.keys())
    logger.info("Uncompressed zip file %s. Contents:\n\t%s" %
                (filename, "\t".join(contents_filenames)))
    return contents_filenames
