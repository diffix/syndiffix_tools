import random
import string

from syndiffix import Synthesizer
from syndiffix.tree import Branch, Leaf, Node


class TreeWalker:
    def __init__(self, sdx: Synthesizer):
        self.sdx = sdx

    def forest_walker(self):
        for col_id, root in self.sdx.forest._tree_cache.items():
            yield col_id, root

    def tree_walker(self, node: Node, parent: Node = None):
        nodes = []
        if isinstance(node, Leaf):
            nodes.append([node, parent])
        elif isinstance(node, Branch):
            nodes.append([node, parent])
            for child_node in node.children.values():
                nodes += self.tree_walker(node=child_node, parent=node)
        return nodes

    def node_info(self, node: Node, parent: Node = None) -> dict:
        actual_intervals = []
        for ai in node.actual_intervals:
            actual_intervals.append([ai.min, ai.max])
        snapped_intervals = []
        for si in node.snapped_intervals:
            snapped_intervals.append([si.min, si.max])
        low_threshold = (
            node.context.anonymization_context.anonymization_params.low_count_params.low_threshold
        )
        comb = node.context.combination
        subnode_ids = []
        for subnode in node.subnodes:
            subnode_ids.append(self._make_node_id(subnode))
        columns = []
        for col_index in comb:
            columns.append(self.sdx.forest.columns[col_index])
        info = {
            "node_id": self._make_node_id(node),
            "par_id": self._make_node_id(parent),
            "columns": columns,
            "combination": comb,
            "actual_intervals": actual_intervals,
            "snapped_intervals": snapped_intervals,
            "singularity": node.is_singularity(),
            "over_threshold": node.is_over_threshold(low_threshold),
            "noisy_count": node.noisy_count(),
            "subnodes": subnode_ids,
        }
        if isinstance(node, Leaf):
            info["node_type"] = "leaf"
            info["true_count"] = len(node.rows)
        else:
            info["node_type"] = "branch"
            child_ids = []
            for child_node in node.children.values():
                child_ids.append(self._make_node_id(child_node))
            info["children"] = child_ids
        return info

    def _make_node_id(self, node: Node) -> str:
        if node is None:
            return "nid:000000"
        seed = ""
        for col_index in node.context.combination:
            seed += str(col_index) + "_"
        for si in node.snapped_intervals:
            seed += str(si.min) + "_" + str(si.max) + "_"
        random.seed(seed)
        return "nid:" + "".join(
            random.choices(string.ascii_lowercase + string.digits, k=6)
        )

    def get_forest_nodes(self):
        forest = {}
        for col_id, root in self.forest_walker():
            for node, parent in self.tree_walker(root):
                ni = self.node_info(node=node, parent=parent)
                forest[ni["node_id"]] = ni
        return forest
