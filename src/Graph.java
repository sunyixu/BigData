import org.apache.hadoop.io.Text;

import java.util.*;
import java.util.Map;

public class Graph {
    private int n;
    private int[][] adjArray;
    HashMap<Integer, Integer> new_old_map = new HashMap<>();

    public Graph(Iterable<Text> edges) {

        HashMap<Integer, ArrayList<Integer>> neighbours = new HashMap<>();
        String[] nodes;
        int node1, node2;
        for(Text e: edges){
            nodes = e.toString().split(",");
            node1 = Integer.parseInt(nodes[0]);
            node2 = Integer.parseInt(nodes[1]);

            this.addNodes(node1, node2, neighbours);
            this.addNodes(node2, node1, neighbours);
        }

        n = neighbours.size();
        adjArray = new int[n][];
        this.orderRenamebyNeighbours(neighbours);

        //printAdj(adjArray);

    }

    public Graph(String[] edges) {

        HashMap<Integer, ArrayList<Integer>> neighbours = new HashMap<>();
        String[] nodes;
        int node1, node2;
        for(String e: edges){
            nodes = e.split(",");
            node1 = Integer.parseInt(nodes[0]);
            node2 = Integer.parseInt(nodes[1]);
            this.addNodes(node1, node2, neighbours);
            this.addNodes(node2, node1, neighbours);
        }

        n = neighbours.size();
        adjArray = new int[n][];
        this.orderRenamebyNeighbours(neighbours);

        printAdj(adjArray);

    }

    public HashSet<int[]> compactForward(){
        int sizeV, sizeU;
        HashSet<int[]> result = new HashSet<>();
        int v, u, nextU, nextV;

        for(int i = 0; i < n; i++){
            v = i;
            sizeV = adjArray[i].length;
            for(int j = 0; j < sizeV; j++){
                u = adjArray[i][j];
                int indexU = 0, indexV = 0;
                if(u > v){
                    nextV = adjArray[v][indexV];
                    nextU = adjArray[u][indexU];
                    sizeU = adjArray[u].length;
                    while(indexU < sizeU && indexV < sizeV && nextU < v && nextV < v){
                        if(nextU < nextV){
                            indexU++;
                            nextU = adjArray[u][indexU];
                        }else if(nextU > nextV){
                            indexV++;
                            nextV = adjArray[v][indexV];
                        }else{
                            int[] triangle = {new_old_map.get(u),new_old_map.get(v),new_old_map.get(nextU)};
                            result.add(triangle);

                            indexU++; indexV++;
                            nextU = adjArray[u][indexU];
                            nextV = adjArray[v][indexV];
                        }
                    }
                }
            }

        }

        return result;
    }

    /**
     * Given an hashmap<node, list of his neighbours>, it orders and renames
     * the nodes based on the number of its neighbours in decreasing order,
     * i.e the node with the highest number of neighbours will be node 0,
     * the node with the lowest number of neighbours will be node n-1
     * It then creates the adjacency array of the graph
     * @param neighbours
     */
    private void orderRenamebyNeighbours(HashMap<Integer, ArrayList<Integer>> neighbours){
        HashMap<Integer, Integer> count_neighbours = new HashMap<>();

        for(int key: neighbours.keySet()){
            count_neighbours.put(key, neighbours.get(key).size());
        }

        LinkedHashMap<Integer, Integer> sorted_nodes = sortByValue(count_neighbours);
        HashMap<Integer, Integer> old_new_map = new HashMap<>();

        int i = 0;
        for(Integer node: sorted_nodes.keySet()){
            old_new_map.put(node, i);
            new_old_map.put(i, node);
            //debug
            //System.out.println("old name: " + new_old_map.get(i) + " new name: " + old_new_map.get(node));
            i++;
        }

        for(i = 0; i < n; i++){
            Integer[] n_list = neighbours.get(new_old_map.get(i)).toArray(new Integer[0]);

            for(int j = 0; j < n_list.length; j++){
                //rename all the nodes in decreasing number of neighbours
                n_list[j] = old_new_map.get(n_list[j]);
                //System.out.println(n_list[j]);
            }

            //sort neighbours in increasing order based on the new name
            Arrays.sort(n_list);

            adjArray[i] = new int[n_list.length];
            for (int j = 0; j < n_list.length; j++){
                adjArray[i][j] = n_list[j];
            }
        }
    }


    /**
     * Print the adjacency array of the graph
     * The i-th line of the array represent the neighbours of the i-th node in increasing order
     * @param adjArray
     */
    public void printAdj(int[][] adjArray){
        String s = "";
        for(int i = 0; i < n; i++){
            int size = adjArray[i].length;
            for(int j = 0; j < size; j++){
                s += adjArray[i][j] + ",";
            }
            System.out.println("Node " + i + ": " + s);
            s = "";
        }
    }

    /**
     * Add neighbour to a node
     * @param node
     * @param neighbour
     * @param neighbours
     */
    private void addNodes(int node, int neighbour, HashMap<Integer, ArrayList<Integer>> neighbours){
        if(!neighbours.containsKey(node)){
            neighbours.put(node, new ArrayList<Integer>());
        }

        if(!neighbours.get(node).contains(neighbour)){
            neighbours.get(node).add(neighbour);
        }

    }

    /**
     * Sort the map in decreasing order based on the number of neighbours of a node
     * @param map
     * @param <K>
     * @param <V>
     * @return ordered linkedHashMap
     */
    private static <K, V extends Comparable<? super V>> LinkedHashMap<K, V> sortByValue(Map<K, V> map) {
        List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                int result = o1.getValue().compareTo(o2.getValue());
                if (result == 0){
                    return result;
                }
                if (result == -1){
                    return 1;
                }

                return -1;
            }
        });

        LinkedHashMap<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

}
