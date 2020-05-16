package helpers;

import java.io.InvalidClassException;

/**
 * Interface used to provide Mappers a generic behaviour letting them act in different scenarios or concatenate them in
 * different ways.
 * @param <K> KeyInput
 * @param <V> ValueInput
 * @param <T> Parsing object: Once params K and V are set, param T represents the output of the parsing activity and the
 *           object which the Mapper will use during the map() activity.
 */
public interface MultiInputMapper<K, V, T> {
    T parseInput(K key, V value) throws InvalidClassException;
}
