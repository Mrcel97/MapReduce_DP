package helpers;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.util.Pair;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.List;

public class SortedTreeMap<K, V> {
    public ObservableList<K> keys = FXCollections.observableArrayList();
    List<Pair<K, V>> map = new ArrayList<>();

    public SortedTreeMap() {
    }

    public void emptyEntry(K key) {
        this.map.add(new Pair<>(key, null));
    }

    public void append(K key, V value) {
        this.keys.add(key);
        Pair<K, V> insertion = new Pair<>(key, value);
        this.map.add(insertion);
    }

    public List<K> getSortedKeys() {
        return keys.sorted();
    }

    public List<Pair<K,V>> getMap() {
        return map;
    }

    public Pair<K, V> extract(K key) {
        Pair<K, V> extraction;
        for (Pair<K, V> item : map) {
            if (item.getKey() == key) {
                extraction = item;
                this.map.remove(new Pair<>(item.getKey(), item.getValue()));
                return extraction;
            }
        }
        return null;
    }

    public int size() {
        return map.size();
    }

    public Text pairToText(Pair<K, V> input) {
        return new Text(input.getKey().toString() + "," + input.getValue().toString());
    }
}
