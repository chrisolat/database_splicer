def topological_sort(digraph):
    visited = {node: False for node in digraph}
    result = []
    def dfs(node):
        if visited[node] == "Done":
            return True
        if visited[node]:
            return False
        visited[node] = True
        for i in digraph[node]:
            check = dfs(i)
            if not check:
                return False
        visited[node] = "Done"
        result.append(node)
        return True
    
    for node in digraph:
        check = dfs(node)
        if not check:
            return [] # return empty list if cycle is detected
    return result[::-1]
        


adjacency_list = {
    "tree1": [],
    "tree2": [
        "tree1"
    ],
    "tree3": [
        "tree1"
    ],
    "tree4": [],
    "tree5": [
        "tree4"
    ]
}
if __name__ == "__main__":
    print(topological_sort(adjacency_list))