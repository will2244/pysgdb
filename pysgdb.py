import os
import pickle 
from copy import deepcopy
from typing import Any, List, Tuple, Set


# helper functions
def _unique_elements(a: List[Any], b: List[Any]) -> Tuple[List[Any], List[Any]]:
    b_copy = deepcopy(b)
    c = []
    for item in a:
        if item in b_copy:
            b_copy.remove(item)
        else:
            c.append(item)
    d = [item for item in b_copy if item not in a]
    return c, d

def _unique_tuples(a: Set[Tuple[Any, Any]], b: Set[Tuple[Any, Any]]) -> Tuple[List[Tuple[Any, Any]], List[Tuple[Any, Any]]]:
    c = list(a - b)
    d = list(b - a)
    return c, d


Id = str

class DB:

    def __init__(self):
        pass


    def _init_schema(self, schema: dict):
        self.db = {
            "schema": {},
            "nodes": {},       # db[nodes][node_name] -> Dict[attr_name, value]
            "->": {},          # db[direction][source_node_name][id][target_node_name] -> Set[Id]
            "<-": {},          # db[direction][target_node_name][id][source_node_name] -> Set[Id]
            "node_links": {},  # db[node_links][source_node_name] -> Set[link_name]
            "current_id": "0"
        }
        self.db["schema"] = schema # TODO: make sure 'schema' is the right shape
        
        for node_name in self.db["schema"]["nodes"].keys():
            self.db["nodes"][node_name] = {}
        
        for (source, link, target) in self.db["schema"]["links"]:
            if link not in self.db["->"]:
                self.db["->"][link] = {}
            self.db["->"][link][source] = {}

            if link not in self.db["<-"]:
                self.db["<-"][link] = {}
            self.db["<-"][link][target] = {}

            if source not in self.db["node_links"]:
                self.db["node_links"][source] = set()
            self.db["node_links"][source].add(link)


    def _update_schema(self, schema: dict):

        ### validations ###
        current_links = self.db["schema"]["links"]
        target_links = schema["links"]
        deleted_links, new_links = _unique_tuples(current_links, target_links)
        deleted_link_names = [x[1] for x in deleted_links]

        for (source, link, target) in deleted_links:
            # check that there are no connections for the given link
            remaining_forward_connections = len(self.db["->"][link][source])
            assert remaining_forward_connections == 0, f"Cannot update schema. link ({link}: {source} -> {target}) still has '{remaining_forward_connections}' remaining connections. Please delete these first before migrating."
            remaining_backward_connections = len(self.db["<-"][link][target])
            assert remaining_backward_connections == 0, f"Cannot update schema. link ({link}: {target} <- {source}) still has '{remaining_backward_connections}' remaining connections. Please delete these first before migrating."
            
        current_nodes = list(self.db["schema"]["nodes"].keys())
        target_nodes = list(schema["nodes"].keys())
        deleted_nodes, new_nodes = _unique_elements(current_nodes, target_nodes)

        for node_name in deleted_nodes:
            # make sure the delete node is not in a link - either forwards or backwards
            found_in_links = list(self.db["node_links"][node_name])
            found_in_links = [x for x in found_in_links if x not in deleted_link_names]
            assert len(found_in_links) == 0, f"Cannot delete node: '{node_name}' because it is still used in links: {found_in_links}"

            # make sure node does not have any objects in it
            num_node_objects = len(self.db["nodes"][node_name])
            assert num_node_objects == 0, f"Cannot delete node: '{node_name}' becuase there are still '{num_node_objects}' objects contained in it"

        ### modifications ###
        
        # 1) Remove links
        for (source, link, target) in deleted_links:

            # schema
            self.db["schema"]["links"].remove((source, link, target))

            # ->
            del self.db["->"][link][source]
            if len(self.db["->"][link]) == 0:
                del self.db["->"][link]

            # <-
            del self.db["<-"][link][target]
            if len(self.db["<-"][link]) == 0:
                del self.db["<-"][link]
            
            # node_links
            self.db["node_links"][source].remove(link)
            if len(self.db["node_links"][source]) == 0:
                del self.db["node_links"][source]

        # 2) Remove nodes
        for node_name in deleted_nodes:
            del self.db["schema"]["nodes"][node_name]
            del self.db["nodes"][node_name]

        # 3) Add nodes
        for node_name in new_nodes:
            # add the node to the schema
            self.db["schema"]["nodes"][node_name] = schema["nodes"][node_name]
            self.db["nodes"][node_name] = {}

        # 4) Add links
        for (source, link, target) in new_links:
            
            # schema
            self.db["schema"]["links"].add((source, link, target))

            # ->
            if link not in self.db["->"]:
                self.db["->"][link] = {}
            self.db["->"][link][source] = {}

            # <-
            if link not in self.db["<-"]:
                self.db["<-"][link] = {}
            self.db["<-"][link][target] = {}

            # node_links
            if source not in self.db["node_links"]:
                self.db["node_links"][source] = set()
            self.db["node_links"][source].add(link)



    def migrate(self, schema):
        schema = deepcopy(schema)
        if not hasattr(self, 'db'):
            self._init_schema(schema)
        else:
            self._update_schema(schema)


    def get_id(self) -> str:
        current_id = int(self.db["current_id"])
        self.db["current_id"] = str(current_id + 1)
        return str(current_id)


    def create(self, node_name: str, attributes: [dict]) -> [Id]:
        # 1) Check if the attributes are correct on write (only checks the first dictionary for speed purposes)
        assert len(attributes) > 0, "Must send at least one set of attributes to the create() function"
        assert node_name in self.db["schema"]["nodes"], "Node name does not exist in schema"
        first_attribute_set = attributes[0]
        assert len(first_attribute_set) == len(self.db["schema"]["nodes"][node_name]), "Wrong number or attributes given in create()"
        
        # 2) For each attr: check if the types are aligned
        for attribute_name, attribute_type in self.db["schema"]["nodes"][node_name].items():
            assert attribute_name in first_attribute_set, f"Wrong attribute name found when creating a node in create(): {attribute_name}"
            assert type(first_attribute_set[attribute_name]).__name__ == attribute_type, "Type mismatch in db create()"

        # 3) Save new entity to db
        new_ids = []
        for attribute_set in attributes:
            new_id = self.get_id()
            self.db["nodes"][node_name][new_id] = attribute_set
            new_ids.append(new_id)
        return new_ids


    def delete(self, node_name: str, ids: [Id]):
        assert node_name in self.db["schema"]["nodes"], f"Node name: {node_name} not in schema"
        
        for node_id in ids:
            # validate if the node_id exists
            assert node_id in self.db["nodes"][node_name], f"ID: {node_id} not found in {node_name} nodes"
            
            # delete links in both directions
            links_to_check = self.db["node_links"].get(node_name, set())
            for link in links_to_check:
                for direction, opposite_direction in [("->", "<-"), ("<-", "->")]: # TODO check for bug, opposite_direction
                    targets_to_unlink = []
                    try:
                        for target_node, target_ids in self.db[direction][link][node_name][node_id].items():
                            targets_to_unlink.extend((target_node, tid) for tid in target_ids)
                    except KeyError:
                        continue  # No links in this direction for this node
                    
                    # Using self.unlink to remove links
                    for target_node, target_id in targets_to_unlink:
                        self.unlink(node_name, [node_id], link, target_node, [target_id])
            
            # delete node from db["nodes"]
            del self.db["nodes"][node_name][node_id]


    def link(self, node_1_name: str, node_1_ids: [Id], link: str, node_2_name: str, node_2_ids: [Id]):
        # Validate if node names and link are in schema
        assert node_1_name in self.db["schema"]["nodes"], f"Cannot create link, node name: {node_1_name} not in schema"
        assert node_2_name in self.db["schema"]["nodes"], f"Cannot create link, node name: {node_2_name} not in schema"
        assert (node_1_name, link, node_2_name) in self.db["schema"]["links"], f"Cannot create link, link name: {link} not in schema"
        # TODO: above assert may be slow - check

        # Loop through each ID in node_1_ids and node_2_ids and create links
        for node_1_id in node_1_ids:
            # Make sure the ID exists in the nodes
            assert node_1_id in self.db["nodes"][node_1_name], f"ID: {node_1_id} not found in {node_1_name} nodes"
            
            for node_2_id in node_2_ids:
                # Make sure the ID exists in the nodes
                assert node_2_id in self.db["nodes"][node_2_name], f"ID: {node_2_id} not found in {node_2_name} nodes"
                
                # Create links for the -> direction
                if node_1_id not in self.db["->"][link][node_1_name]:
                    self.db["->"][link][node_1_name][node_1_id] = {}
                if node_2_name not in self.db["->"][link][node_1_name][node_1_id]:
                    self.db["->"][link][node_1_name][node_1_id][node_2_name] = set()
                self.db["->"][link][node_1_name][node_1_id][node_2_name].add(node_2_id)
                
                # Create links for the <- direction
                if node_2_id not in self.db["<-"][link][node_2_name]:
                    self.db["<-"][link][node_2_name][node_2_id] = {}
                if node_1_name not in self.db["<-"][link][node_2_name][node_2_id]:
                    self.db["<-"][link][node_2_name][node_2_id][node_1_name] = set()
                self.db["<-"][link][node_2_name][node_2_id][node_1_name].add(node_1_id)


    def unlink(self, node_1_name: str, node_1_ids: [Id], link: str, node_2_name: str, node_2_ids: [Id]):
        assert node_1_name in self.db["schema"]["nodes"], f"Node name: {node_1_name} not in schema"
        assert node_2_name in self.db["schema"]["nodes"], f"Node name: {node_2_name} not in schema"
        assert (node_1_name, link, node_2_name) in self.db["schema"]["links"], f"Link: {link} not in schema between {node_1_name} and {node_2_name}"
        
        for node_1_id in node_1_ids:
            assert node_1_id in self.db["nodes"][node_1_name], f"ID: {node_1_id} not found in {node_1_name} nodes"
            
            for node_2_id in node_2_ids:
                assert node_2_id in self.db["nodes"][node_2_name], f"ID: {node_2_id} not found in {node_2_name} nodes"
                
                try:
                    self.db["->"][link][node_1_name][node_1_id][node_2_name].remove(node_2_id)
                    if not self.db["->"][link][node_1_name][node_1_id][node_2_name]:
                        del self.db["->"][link][node_1_name][node_1_id][node_2_name]
                        
                    if not self.db["->"][link][node_1_name][node_1_id]:
                        del self.db["->"][link][node_1_name][node_1_id]
                except KeyError:
                    pass  # Link didn’t exist, so nothing to unlink
                
                try:
                    self.db["<-"][link][node_2_name][node_2_id][node_1_name].remove(node_1_id)
                    if not self.db["<-"][link][node_2_name][node_2_id][node_1_name]:
                        del self.db["<-"][link][node_2_name][node_2_id][node_1_name]
                        
                    if not self.db["<-"][link][node_2_name][node_2_id]:
                        del self.db["<-"][link][node_2_name][node_2_id]
                except KeyError:
                    pass  # Link didn’t exist, so nothing to unlink


    def get_data(self, node_name: str, ids: [Id], attributes: [str]) -> [[Any]]:
        assert node_name in self.db["schema"]["nodes"], f"Node name: {node_name} not in schema"
        for attr in attributes:
            assert attr in self.db["schema"]["nodes"][node_name], f"Attribute: {attr} not found in {node_name} nodes"
            
        result = []
        for node_id in ids:
            assert node_id in self.db["nodes"][node_name], f"ID: {node_id} not found in {node_name} nodes"
            entity_data = []
            for attr in attributes:
                entity_data.append(self.db["nodes"][node_name][node_id].get(attr, None))
            result.append(entity_data)
        return result


    def traverse(self, source_node_name: str, source_ids: [Id], direction: str, link_name: str, target_node_name: str) -> [Id]:
        assert source_node_name in self.db["schema"]["nodes"], f"Source node name: {source_node_name} not in schema"
        assert target_node_name in self.db["schema"]["nodes"], f"Target node name: {target_node_name} not in schema"
        assert direction in ["->", "<-"], f"Invalid direction: {direction}. Must be '->' or '<-'."
        assert all(source_id in self.db["nodes"][source_node_name] for source_id in source_ids), f"Some source IDs not found in {source_node_name} nodes"
        assert (source_node_name, link_name, target_node_name) in self.db["schema"]["links"] or \
            (target_node_name, link_name, source_node_name) in self.db["schema"]["links"], f"Link: {link_name} not in schema between {source_node_name} and {target_node_name}"

        results = set()
        for source_id in source_ids:
            try:
                targets = self.db[direction][link_name][source_node_name][source_id].get(target_node_name, set())
                results.update(targets)
            except KeyError:
                # No links for this source_id, skip to the next one
                continue
        return list(results)


    def save(self, folder_path: str, db_filename: str):
        with open(os.path.join(folder_path, db_filename), 'wb') as f:
            pickle.dump(self.db, f)


    def load(self, folder_path: str, db_filename: str):
        with open(os.path.join(folder_path, db_filename), 'rb') as f:
            self.db = pickle.load(f)