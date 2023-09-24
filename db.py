import copy
from datetime import datetime
from typing import Dict, Any

Id = str

class DB:
    def __init__(self, schema):
        self.db = {
            "schema": {},
            "nodes": {},       # db[nodes][node_name] -> Dict[attr_name, value]
            "->": {},          # db[direction][source_node_name][id][target_node_name] -> Set[Id]
            "<-": {},
            "node_links": {},  # db[node_links][source_node_name] -> Set[link_name]
            "current_id": "0"
        }
        self.db["schema"] = copy.deepcopy(schema)
        
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


    def get_id(self) -> str:
        current_id = int(self.db["current_id"])
        self.db["current_id"] = str(current_id + 1)
        return str(current_id)


    def create(self, node_name: str, attributes: [dict]):
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
        for attribute_set in attributes:
            self.db["nodes"][node_name][self.get_id()] = attribute_set


    def delete(self, node_name: str, ids: [Id]):
        assert node_name in self.db["schema"]["nodes"], f"Node name: {node_name} not in schema"
        
        for node_id in ids:
            # validate if the node_id exists
            assert node_id in self.db["nodes"][node_name], f"ID: {node_id} not found in {node_name} nodes"
            
            # delete links in both directions
            links_to_check = self.db["node_links"].get(node_name, set())
            for link in links_to_check:
                for direction, opposite_direction in [("->", "<-"), ("<-", "->")]:
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


    def get_data(self, node_name: str, ids: [Id], attributes: [[str]]) -> [Dict[Id, Any]]:
        pass


    def traverse(self, starting_ids: [str], direction: str, link_name: str, node_name: str) -> [Id]:
        pass