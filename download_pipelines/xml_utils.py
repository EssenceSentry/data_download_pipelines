from typing import Dict, List, Optional
from xml.etree import ElementTree


def to_list(branch):
        if branch.text and branch.text.strip():
            return [branch.tag, branch.text]
        else:
            return [branch.tag, [to_list(sub_branch) for sub_branch in branch]]


def to_dict(list_):
    if list_ and isinstance(list_, list):
        if isinstance(list_[0], str):
            # a list with first elem str is a dict
            return {list_[0]: to_dict(list_[1])}
        elif isinstance(list_[0], list):
            if len(list_) == 1:
                # a list, first and only elem also a list is a dict
                return to_dict(list_[0])
            else:
                # a list, first elem is list, more than one elem, then it is a list of dicts
                part = [to_dict(e) for e in list_]
                if isinstance(part[0], dict):
                    # first elem is dict, then all dict
                    ks = [[k for k in d][0] for d in part]
                    vs = [[d[k] for k in d][0] for d in part]
                    if len(ks) > 1:
                        # it has more than one key
                        if ks[0] != ks[1]:
                            # the keys are distinct, then normal dict
                            d = dict(zip(ks, vs))
                        else:
                            # first two keys are equal, then a dict with one key and list of values
                            d = {ks[0]: vs}
                        return d
                return part
    if list_ and isinstance(list_, str):
        # a string is returned as is
        return list_
    else:
        # anything else is an empty str
        return ""


def xml_to_dict(root):
    return to_dict(to_list(root))


def xml_findall_deep(filename: str,
                     tag: Optional[str] = None) -> List[Dict[str, str]]:
    root = ElementTree.parse(filename).getroot()
    if not tag:
        return xml_to_dict(root)
    result: List = []
    branches: List = [root]
    while branches and not result:
        sub_branches = []
        for branch in branches:
            r = branch.findall(tag)
            if r:
                result += r
            else:
                sub_branches += [b for b in branch]
        if not result:
            branches = sub_branches
    return [xml_to_dict(elem)[tag] for elem in result]
