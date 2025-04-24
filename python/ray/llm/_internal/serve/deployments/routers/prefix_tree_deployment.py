from ray import serve
import time
from collections import defaultdict
from threading import RLock
import json


class Node:
    """
    Node in a prefix tree that tracks tenant access time.
    
    Each node represents a segment of text and can belong to multiple tenants.
    """
    def __init__(self, text="", parent=None):
        self.text = text
        self.parent = parent
        self.children = {}  # Maps char -> Node
        self.tenant_last_access_time = {}  # Maps tenant -> timestamp
        self.lock = RLock()  # For thread safety
        
    def __str__(self):
        return f"Node(text='{self.text}', tenants={list(self.tenant_last_access_time.keys())})"

@serve.deployment(name="TreeDeployment")
class PrefixTree:
    """
    Thread-safe multi-tenant prefix tree (approximate radix tree).
    
    Features:
    1. Stores data for multiple tenants in the same tree structure
    2. Node-level locking for concurrent access
    3. Leaf LRU eviction based on tenant access time
    """
    def __init__(self):
        self.root = Node()
        self.tenant_char_count = defaultdict(int)  # Maps tenant -> character count
        self.lock = RLock()  # For operations that need to lock the entire tree
 
    def hello(self, input_str="World", output_file=None, request_id=None):
        # Record time when the request is received by the deployment
        received_time = time.time()
        
        if output_file and request_id is not None:
            # Write timing data in JSON format
            data = {
                "request_id": request_id,
                "stage": "received_by_deployment",
                "timestamp": received_time
            }
            with open(output_file, "a") as f:
                f.write(json.dumps(data) + "\n")
        
        
        # Simulate some processing overhead
        for _ in range(1000):
            pass
            
        # Record time after completing the function logic
        completed_time = time.time()
        
        if output_file and request_id is not None:
            # Write timing data in JSON format
            data = {
                "request_id": request_id,
                "stage": "completed_function_logic",
                "timestamp": completed_time
            }
            with open(output_file, "a") as f:
                f.write(json.dumps(data) + "\n")        
        return f"Hello {input_str}"

    @staticmethod
    def shared_prefix_count(a, b):
        """Count the number of shared characters at the beginning of two strings."""
        i = 0
        for char_a, char_b in zip(a, b):
            if char_a == char_b:
                i += 1
            else:
                break
        return i

    # TODO: Verify that updating access time behavior is correct.
    def insert(self, text, tenant, output_file=None, request_id=None):
        """Insert text into tree with given tenant."""
        # Record time when the insert method is called
        timing = {"received_by_insert": time.time()}
        try:
            with self.lock:
                curr_node = self.root
                timestamp_ms = int(time.time() * 1000)
                i = 0
                while i < len(text):
                    curr_node.tenant_last_access_time[tenant] = timestamp_ms
                    first_char = text[i]
                    curr_text = text[i:]
                    if first_char not in curr_node.children:
                        # No match, create new node
                        # e.g. curr_node.children = {}, curr_text = "hello" -> curr_node.children = {"h": Node("hello")}
                        new_node = Node(text=curr_text, parent=curr_node)
                        new_node.tenant_last_access_time[tenant] = timestamp_ms
                        
                        # Increment char count for tenant
                        self.tenant_char_count[tenant] += len(curr_text)
                        
                        curr_node.children[first_char] = new_node
                        return True, timing
                    else:
                        # Match found, check if need to split
                        matched_node = curr_node.children[first_char]
                        shared_count = self.shared_prefix_count(matched_node.text, curr_text)
                        
                        if shared_count < len(matched_node.text):
                            # Partial match, split at matched point
                            # Example:
                            ## Before update:
                            ### curr_node.children = {"h": Node("helloworld")}, curr_text = "hellothere" -> shared_count = 6
                            ### matched_node = Node("helloworld")

                            ## During update:
                            ### Increment tenant_char_count[tenant] by shared_count if matched_node has not seen this tenant before

                            ## After update:
                            ### curr_node.children = {"h": Node("hello", children = {"w": Node("world")})}
                            ### parent_node = Node("hello"), matched_node = Node("world")
                            ### Update tenant_last_access_time for parent_node, NOT matched_node
                            ### (new) curr_text = "there", (new) curr_node = parent_node
                            ### Continue adding "there" to tree in next iteration

                            matched_text = matched_node.text[:shared_count]
                            remaining_text = matched_node.text[shared_count:]

                            # Update tenant char count for the new split node
                            if tenant not in matched_node.tenant_last_access_time:
                                self.tenant_char_count[tenant] += shared_count
                            
                            # Create new parent node
                            new_parent = Node(text=matched_text, parent=curr_node)
                            new_parent.tenant_last_access_time = matched_node.tenant_last_access_time.copy()
                            # new_parent.tenant_last_access_time[tenant] = timestamp_ms
                            
                            # Update matched_node
                            matched_node.text = remaining_text
                            matched_node.parent = new_parent

                            # Connect new parent node to matched_node
                            new_parent.children[remaining_text[0]] = matched_node

                            # Connect current node to new parent
                            curr_node.children[first_char] = new_parent
                            
                            # Move down the tree
                            curr_node = new_parent
                            i += shared_count
                        else:
                            # Full match

                            # Update tenant char count if this is a new tenant for this node
                            if tenant not in matched_node.tenant_last_access_time:
                                self.tenant_char_count[tenant] += shared_count
                            
                            # # Update tenant last access time
                            # matched_node.tenant_last_access_time[tenant] = timestamp_ms

                            # Move down the tree
                            curr_node = matched_node
                            i += shared_count
            return True, timing
        finally:
            # Record completion time - this will execute regardless of how the function exits
            # (both normal return and exception cases)
            timing["completed_insert"] = time.time()

    # # NOTE: This is AI generated. Need to verify that it works (I don't really like the recursive approach).
    # def prefix_match_generator(self, text):
    #     """
    #     Match text against tree and return a generator of (matched_text, matched_tenant), 
    #     in order from most matched to least matched.
    #     If a tenant has multiple matches, only return the best match with text.
    #     So should yield at most T matches, where T is the number of tenants.
    #     """
    #     # Dictionary to track best match for each tenant
    #     best_matches = {}  # tenant -> (match_length, matched_text)
        
    #     # Start from the root
    #     def traverse(node, current_text="", matched_text=""):
    #         # Check if this node belongs to any tenants
    #         for tenant in node.tenant_last_access_time:
    #             current_match_length = len(matched_text)
                
    #             # If we've found a better match for this tenant, update it
    #             if tenant not in best_matches or current_match_length > best_matches[tenant][0]:
    #                 best_matches[tenant] = (current_match_length, matched_text)
            
    #         # If we've processed the entire input text, stop
    #         if not current_text:
    #             return
            
    #         # Get the first character of remaining text
    #         first_char = current_text[0]
            
    #         # Check if there's a matching child
    #         if first_char in node.children:
    #             child = node.children[first_char]
                
    #             # Find how much of the child's text matches the current text
    #             shared_count = self.shared_prefix_count(child.text, current_text)
                
    #             # If we have a partial or full match, continue traversal
    #             if shared_count > 0:
    #                 next_matched = matched_text + current_text[:shared_count]
    #                 next_text = current_text[shared_count:]
    #                 traverse(child, next_text, next_matched)
        
    #     # Start traversal from root
    #     traverse(self.root, text, "")
        
    #     # Sort matches by length (most matched chars first)
    #     sorted_matches = sorted(
    #         [(tenant, match_len, match_text) for tenant, (match_len, match_text) in best_matches.items()],
    #         key=lambda x: x[1],
    #         reverse=True
    #     )
    #     if not sorted_matches:
    #         yield ("", "empty")
    #     # Yield results in order
    #     for tenant, _, match_text in sorted_matches:
    #         yield (match_text, tenant)

    def prefix_match_generator(self, text):
        """
        Generator-based version of prefix_match.
        Yields (matched_text, matched_tenant) at each step.
        If the tree changes, it will adjust dynamically.
        """
        num_retries = 0
        while num_retries < len(self.tenant_char_count):  # Keep retrying with updated tree state
            num_retries += 1
            curr = self.root
            curr_idx = 0
            prev = self.root
            text_len = len(text)

            while curr_idx < text_len:
                first_char = text[curr_idx]
                curr_text = text[curr_idx:]
                curr = prev  # Always start from the previous best match

                with curr.lock:
                    if first_char in curr.children:
                        matched_node = curr.children[first_char]
                        
                        with matched_node.lock:
                            shared_count = self.shared_prefix_count(matched_node.text, curr_text)
                            matched_node_text_len = len(matched_node.text)

                            if shared_count == matched_node_text_len:
                                # Full match with current node's text, continue to next node
                                curr_idx += shared_count
                                prev = matched_node
                            else:
                                # Partial match, stop here
                                curr_idx += shared_count
                                prev = matched_node
                                break
                    else:
                        # No match found, stop here
                        break

            curr = prev  # Final matched node

            # Select the first tenant
            with curr.lock:
                if curr.tenant_last_access_time:
                    tenant = next(iter(curr.tenant_last_access_time))
                else:
                    tenant = "empty"

            # Update timestamp for all nodes from match point to root
            if tenant != "empty":
                timestamp_ms = int(time.time() * 1000)
                current_node = curr
                while current_node is not None:
                    with current_node.lock:
                        current_node.tenant_last_access_time[tenant] = timestamp_ms
                    current_node = current_node.parent
            
            ret_text = text[:curr_idx]

            # Yield current best match and tenant
            yield ret_text, tenant

            # If resumed, **recompute traversal** in case tree structure has changed


    def prefix_match(self, text, output_file=None, request_id=None):
        """
        Match text against tree and return (matched_text, matched_tenant).
        Updates access time for the matched tenant.
        """
        # Record time when the prefix_match method is called
        timing = {"received_by_prefix_match": time.time()}
        try:
            curr = self.root
            curr_idx = 0
            prev = self.root
            text_len = len(text)
            
            while curr_idx < text_len:
                first_char = text[curr_idx]
                curr_text = text[curr_idx:]
                
                curr = prev
                
                with curr.lock:
                    if first_char in curr.children:
                        matched_node = curr.children[first_char]
                        
                        with matched_node.lock:
                            shared_count = self.shared_prefix_count(matched_node.text, curr_text)
                            matched_node_text_len = len(matched_node.text)
                            
                            if shared_count == matched_node_text_len:
                                # Full match with current node's text, continue to next node
                                curr_idx += shared_count
                                prev = matched_node
                            else:
                                # Partial match, stop here
                                curr_idx += shared_count
                                prev = matched_node
                                break
                    else:
                        # No match found, stop here
                        break
            
            curr = prev
            
            # Select the first tenant
            with curr.lock:
                if curr.tenant_last_access_time:
                    tenant = next(iter(curr.tenant_last_access_time))
                else:
                    tenant = "empty"
            
            # Update timestamp for all nodes from match point to root
            if tenant != "empty":
                timestamp_ms = int(time.time() * 1000)
                current_node = curr
                
                while current_node is not None:
                    with current_node.lock:
                        current_node.tenant_last_access_time[tenant] = timestamp_ms
                    current_node = current_node.parent
            
            ret_text = text[:curr_idx]
            return ret_text, tenant, timing
        
        finally:
            # Record completion time - this will execute regardless of how the function exits
            timing["completed_prefix_match"] = time.time()
    
    def prefix_match_tenant(self, text, tenant):
        """
        Match text against tree for a specific tenant and return matched text.
        Updates access time for the tenant.
        """
        curr = self.root
        curr_idx = 0
        prev = self.root
        text_len = len(text)
        
        while curr_idx < text_len:
            first_char = text[curr_idx]
            curr_text = text[curr_idx:]
            
            curr = prev
            
            with curr.lock:
                if first_char in curr.children:
                    matched_node = curr.children[first_char]
                    
                    with matched_node.lock:
                        # Only continue matching if this node belongs to the specified tenant
                        if tenant not in matched_node.tenant_last_access_time:
                            break
                            
                        shared_count = self.shared_prefix_count(matched_node.text, curr_text)
                        matched_node_text_len = len(matched_node.text)
                        
                        if shared_count == matched_node_text_len:
                            # Full match with current node's text, continue to next node
                            curr_idx += shared_count
                            prev = matched_node
                        else:
                            # Partial match, stop here
                            curr_idx += shared_count
                            prev = matched_node
                            break
                else:
                    # No match found, stop here
                    break
        
        # Update timestamps
        timestamp_ms = int(time.time() * 1000)
        current_node = prev
        
        while current_node is not None:
            with current_node.lock:
                if tenant in current_node.tenant_last_access_time:
                    current_node.tenant_last_access_time[tenant] = timestamp_ms
            current_node = current_node.parent
        
        ret_text = text[:curr_idx]
        return ret_text
    
    def get_smallest_tenant(self):
        """Get the tenant with the smallest total character count."""
        with self.lock:
            if not self.tenant_char_count:
                # Return first worker if no data yet
                return "empty"
            
            return min(self.tenant_char_count.items(), key=lambda x: x[1])[0]
    
    def evict_tenant_by_size(self, max_size):
        """Evict nodes for tenants that exceed the maximum tree size."""
        with self.lock:
            # Get total tree size
            total_size = sum(self.tenant_char_count.values())
            
            # If tree is smaller than max size, no need to evict
            if total_size <= max_size:
                return
            
            # Calculate how much we need to evict
            excess = total_size - max_size
            
            # Sort tenants by size (largest first)
            sorted_tenants = sorted(
                self.tenant_char_count.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
            
            # Evict from largest tenants first
            for tenant, size in sorted_tenants:
                # If we've evicted enough, stop
                if excess <= 0:
                    break
                
                # Calculate how much to evict from this tenant
                # Evict at most half of the tenant's size
                evict_amount = min(excess, size // 2)
                
                if evict_amount > 0:
                    # print(f"Evicting {evict_amount} chars from tenant {tenant}")
                    self.tenant_char_count[tenant] -= evict_amount
                    excess -= evict_amount
            
            # print(f"Tree eviction complete. New size: {sum(self.tenant_char_count.values())}")
    
    def get_tenant_char_count(self):
        """Get character count for each tenant."""
        with self.lock:
            return dict(self.tenant_char_count)
            
    def remove_tenant(self, tenant):
        """Remove all nodes belonging to a tenant."""
        # Would require a traversal of the tree and removing the tenant
        # from tenant_last_access_time. Simplifying for now.
        with self.lock:
            if tenant in self.tenant_char_count:
                del self.tenant_char_count[tenant] 
# from ray import serve
# import time
# import asyncio
# from collections import defaultdict



# class Node:
#     """
#     Node in a prefix tree that tracks tenant access time.
    
#     Each node represents a segment of text and can belong to multiple tenants.
#     """
#     def __init__(self, text="", parent=None):
#         self.text = text
#         self.parent = parent
#         self.children = {}  # Maps char -> Node
#         self.tenant_last_access_time = {}  # Maps tenant -> timestamp
#         self.lock = asyncio.Lock()  # For async safety
        
#     def __str__(self):
#         return f"Node(text='{self.text}', tenants={list(self.tenant_last_access_time.keys())})"

# @serve.deployment(name="deploymentTest")
# class PrefixTree:
#     """
#     Async-safe multi-tenant prefix tree (approximate radix tree).
    
#     Features:
#     1. Stores data for multiple tenants in the same tree structure
#     2. Node-level locking for concurrent access
#     3. Leaf LRU eviction based on tenant access time
#     """
#     def __init__(self):
#         self.root = Node()
#         self.tenant_char_count = defaultdict(int)  # Maps tenant -> character count
#         self.lock = asyncio.Lock()  # For operations that need to lock the entire tree
 

#     @staticmethod
#     def shared_prefix_count(a, b):
#         """Count the number of shared characters at the beginning of two strings."""
#         i = 0
#         for char_a, char_b in zip(a, b):
#             if char_a == char_b:
#                 i += 1
#             else:
#                 break
#         return i

#     # TODO: Verify that updating access time behavior is correct.
#     async def insert(self, text, tenant):
#         """Insert text into tree with given tenant."""
#         async with self.lock:
#             curr_node = self.root
#             timestamp_ms = int(time.time() * 1000)
#             i = 0
#             while i < len(text):
#                 curr_node.tenant_last_access_time[tenant] = timestamp_ms
#                 first_char = text[i]
#                 curr_text = text[i:]
#                 if first_char not in curr_node.children:
#                     # No match, create new node
#                     # e.g. curr_node.children = {}, curr_text = "hello" -> curr_node.children = {"h": Node("hello")}
#                     new_node = Node(text=curr_text, parent=curr_node)
#                     new_node.tenant_last_access_time[tenant] = timestamp_ms
                    
#                     # Increment char count for tenant
#                     self.tenant_char_count[tenant] += len(curr_text)
                    
#                     curr_node.children[first_char] = new_node
#                     return
#                 else:
#                     # Match found, check if need to split
#                     matched_node = curr_node.children[first_char]
#                     shared_count = self.shared_prefix_count(matched_node.text, curr_text)
                    
#                     if shared_count < len(matched_node.text):
#                         # Partial match, split at matched point
#                         # Example:
#                         ## Before update:
#                         ### curr_node.children = {"h": Node("helloworld")}, curr_text = "hellothere" -> shared_count = 6
#                         ### matched_node = Node("helloworld")

#                         ## During update:
#                         ### Increment tenant_char_count[tenant] by shared_count if matched_node has not seen this tenant before

#                         ## After update:
#                         ### curr_node.children = {"h": Node("hello", children = {"w": Node("world")})}
#                         ### parent_node = Node("hello"), matched_node = Node("world")
#                         ### Update tenant_last_access_time for parent_node, NOT matched_node
#                         ### (new) curr_text = "there", (new) curr_node = parent_node
#                         ### Continue adding "there" to tree in next iteration

#                         matched_text = matched_node.text[:shared_count]
#                         remaining_text = matched_node.text[shared_count:]

#                         # Update tenant char count for the new split node
#                         if tenant not in matched_node.tenant_last_access_time:
#                             self.tenant_char_count[tenant] += shared_count
                        
#                         # Create new parent node
#                         new_parent = Node(text=matched_text, parent=curr_node)
#                         new_parent.tenant_last_access_time = matched_node.tenant_last_access_time.copy()
#                         # new_parent.tenant_last_access_time[tenant] = timestamp_ms
                        
#                         # Update matched_node
#                         matched_node.text = remaining_text
#                         matched_node.parent = new_parent

#                         # Connect new parent node to matched_node
#                         new_parent.children[remaining_text[0]] = matched_node

#                         # Connect current node to new parent
#                         curr_node.children[first_char] = new_parent
                        
#                         # Move down the tree
#                         curr_node = new_parent
#                         i += shared_count
#                     else:
#                         # Full match

#                         # Update tenant char count if this is a new tenant for this node
#                         if tenant not in matched_node.tenant_last_access_time:
#                             self.tenant_char_count[tenant] += shared_count
                        
#                         # # Update tenant last access time
#                         # matched_node.tenant_last_access_time[tenant] = timestamp_ms

#                         # Move down the tree
#                         curr_node = matched_node
#                         i += shared_count

#     # # NOTE: This is AI generated. Need to verify that it works (I don't really like the recursive approach).
#     # def prefix_match_generator(self, text):
#     #     """
#     #     Match text against tree and return a generator of (matched_text, matched_tenant), 
#     #     in order from most matched to least matched.
#     #     If a tenant has multiple matches, only return the best match with text.
#     #     So should yield at most T matches, where T is the number of tenants.
#     #     """
#     #     # Dictionary to track best match for each tenant
#     #     best_matches = {}  # tenant -> (match_length, matched_text)
        
#     #     # Start from the root
#     #     def traverse(node, current_text="", matched_text=""):
#     #         # Check if this node belongs to any tenants
#     #         for tenant in node.tenant_last_access_time:
#     #             current_match_length = len(matched_text)
                
#     #             # If we've found a better match for this tenant, update it
#     #             if tenant not in best_matches or current_match_length > best_matches[tenant][0]:
#     #                 best_matches[tenant] = (current_match_length, matched_text)
            
#     #         # If we've processed the entire input text, stop
#     #         if not current_text:
#     #             return
            
#     #         # Get the first character of remaining text
#     #         first_char = current_text[0]
            
#     #         # Check if there's a matching child
#     #         if first_char in node.children:
#     #             child = node.children[first_char]
                
#     #             # Find how much of the child's text matches the current text
#     #             shared_count = self.shared_prefix_count(child.text, current_text)
                
#     #             # If we have a partial or full match, continue traversal
#     #             if shared_count > 0:
#     #                 next_matched = matched_text + current_text[:shared_count]
#     #                 next_text = current_text[shared_count:]
#     #                 traverse(child, next_text, next_matched)
        
#     #     # Start traversal from root
#     #     traverse(self.root, text, "")
        
#     #     # Sort matches by length (most matched chars first)
#     #     sorted_matches = sorted(
#     #         [(tenant, match_len, match_text) for tenant, (match_len, match_text) in best_matches.items()],
#     #         key=lambda x: x[1],
#     #         reverse=True
#     #     )
#     #     if not sorted_matches:
#     #         yield ("", "empty")
#     #     # Yield results in order
#     #     for tenant, _, match_text in sorted_matches:
#     #         yield (match_text, tenant)

#     async def prefix_match_generator(self, text):
#         """
#         Generator-based version of prefix_match.
#         Yields (matched_text, matched_tenant) at each step.
#         If the tree changes, it will adjust dynamically.
#         """
#         num_retries = 0
#         while num_retries < len(self.tenant_char_count):  # Keep retrying with updated tree state
#             num_retries += 1
#             curr = self.root
#             curr_idx = 0
#             prev = self.root
#             text_len = len(text)

#             while curr_idx < text_len:
#                 first_char = text[curr_idx]
#                 curr_text = text[curr_idx:]
#                 curr = prev  # Always start from the previous best match

#                 async with curr.lock:
#                     if first_char in curr.children:
#                         matched_node = curr.children[first_char]
                        
#                         async with matched_node.lock:
#                             shared_count = self.shared_prefix_count(matched_node.text, curr_text)
#                             matched_node_text_len = len(matched_node.text)

#                             if shared_count == matched_node_text_len:
#                                 # Full match with current node's text, continue to next node
#                                 curr_idx += shared_count
#                                 prev = matched_node
#                             else:
#                                 # Partial match, stop here
#                                 curr_idx += shared_count
#                                 prev = matched_node
#                                 break
#                     else:
#                         # No match found, stop here
#                         break

#             curr = prev  # Final matched node

#             # Select the first tenant
#             async with curr.lock:
#                 if curr.tenant_last_access_time:
#                     tenant = next(iter(curr.tenant_last_access_time))
#                 else:
#                     tenant = "empty"

#             # Update timestamp for all nodes from match point to root
#             if tenant != "empty":
#                 timestamp_ms = int(time.time() * 1000)
#                 current_node = curr
#                 while current_node is not None:
#                     async with current_node.lock:
#                         current_node.tenant_last_access_time[tenant] = timestamp_ms
#                     current_node = current_node.parent
            
#             ret_text = text[:curr_idx]

#             # Yield current best match and tenant
#             yield ret_text, tenant

#             # If resumed, **recompute traversal** in case tree structure has changed


#     async def prefix_match(self, text):
#         """
#         Match text against tree and return (matched_text, matched_tenant).
#         Updates access time for the matched tenant.
#         """
#         curr = self.root
#         curr_idx = 0
#         prev = self.root
#         text_len = len(text)
        
#         while curr_idx < text_len:
#             first_char = text[curr_idx]
#             curr_text = text[curr_idx:]
            
#             curr = prev
            
#             async with curr.lock:
#                 if first_char in curr.children:
#                     matched_node = curr.children[first_char]
                    
#                     async with matched_node.lock:
#                         shared_count = self.shared_prefix_count(matched_node.text, curr_text)
#                         matched_node_text_len = len(matched_node.text)
                        
#                         if shared_count == matched_node_text_len:
#                             # Full match with current node's text, continue to next node
#                             curr_idx += shared_count
#                             prev = matched_node
#                         else:
#                             # Partial match, stop here
#                             curr_idx += shared_count
#                             prev = matched_node
#                             break
#                 else:
#                     # No match found, stop here
#                     break
        
#         curr = prev
        
#         # Select the first tenant
#         async with curr.lock:
#             if curr.tenant_last_access_time:
#                 tenant = next(iter(curr.tenant_last_access_time))
#             else:
#                 tenant = "empty"
        
#         # Update timestamp for all nodes from match point to root
#         if tenant != "empty":
#             timestamp_ms = int(time.time() * 1000)
#             current_node = curr
            
#             while current_node is not None:
#                 async with current_node.lock:
#                     current_node.tenant_last_access_time[tenant] = timestamp_ms
#                 current_node = current_node.parent
        
#         ret_text = text[:curr_idx]
#         return ret_text, tenant
    
#     async def prefix_match_tenant(self, text, tenant):
#         """
#         Match text against tree for a specific tenant and return matched text.
#         Updates access time for the tenant.
#         """
#         curr = self.root
#         curr_idx = 0
#         prev = self.root
#         text_len = len(text)
        
#         while curr_idx < text_len:
#             first_char = text[curr_idx]
#             curr_text = text[curr_idx:]
            
#             curr = prev
            
#             async with curr.lock:
#                 if first_char in curr.children:
#                     matched_node = curr.children[first_char]
                    
#                     async with matched_node.lock:
#                         # Only continue matching if this node belongs to the specified tenant
#                         if tenant not in matched_node.tenant_last_access_time:
#                             break
                            
#                         shared_count = self.shared_prefix_count(matched_node.text, curr_text)
#                         matched_node_text_len = len(matched_node.text)
                        
#                         if shared_count == matched_node_text_len:
#                             # Full match with current node's text, continue to next node
#                             curr_idx += shared_count
#                             prev = matched_node
#                         else:
#                             # Partial match, stop here
#                             curr_idx += shared_count
#                             prev = matched_node
#                             break
#                 else:
#                     # No match found, stop here
#                     break
        
#         # Update timestamps
#         timestamp_ms = int(time.time() * 1000)
#         current_node = prev
        
#         while current_node is not None:
#             async with current_node.lock:
#                 if tenant in current_node.tenant_last_access_time:
#                     current_node.tenant_last_access_time[tenant] = timestamp_ms
#             current_node = current_node.parent
        
#         ret_text = text[:curr_idx]
#         return ret_text
    
#     async def get_smallest_tenant(self):
#         """Get the tenant with the smallest total character count."""
#         async with self.lock:
#             if not self.tenant_char_count:
#                 # Return first worker if no data yet
#                 return "empty"
            
#             return min(self.tenant_char_count.items(), key=lambda x: x[1])[0]
    
#     async def evict_tenant_by_size(self, max_size):
#         """Evict nodes for tenants that exceed the maximum tree size."""
#         async with self.lock:
#             # Get total tree size
#             total_size = sum(self.tenant_char_count.values())
            
#             # If tree is smaller than max size, no need to evict
#             if total_size <= max_size:
#                 return
            
#             # Calculate how much we need to evict
#             excess = total_size - max_size
            
#             # Sort tenants by size (largest first)
#             sorted_tenants = sorted(
#                 self.tenant_char_count.items(), 
#                 key=lambda x: x[1], 
#                 reverse=True
#             )
            
#             # Evict from largest tenants first
#             for tenant, size in sorted_tenants:
#                 # If we've evicted enough, stop
#                 if excess <= 0:
#                     break
                
#                 # Calculate how much to evict from this tenant
#                 # Evict at most half of the tenant's size
#                 evict_amount = min(excess, size // 2)
                
#                 if evict_amount > 0:
#                     print(f"Evicting {evict_amount} chars from tenant {tenant}")
#                     self.tenant_char_count[tenant] -= evict_amount
#                     excess -= evict_amount
            
#             print(f"Tree eviction complete. New size: {sum(self.tenant_char_count.values())}")
    
#     async def get_tenant_char_count(self):
#         """Get character count for each tenant."""
#         async with self.lock:
#             return dict(self.tenant_char_count)
            
#     async def remove_tenant(self, tenant):
#         """Remove all nodes belonging to a tenant."""
#         # Would require a traversal of the tree and removing the tenant
#         # from tenant_last_access_time. Simplifying for now.
#         async with self.lock:
#             if tenant in self.tenant_char_count:
#                 del self.tenant_char_count[tenant] 