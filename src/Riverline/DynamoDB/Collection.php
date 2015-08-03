<?php

namespace Riverline\DynamoDB;

use Closure;

class Collection implements \IteratorAggregate, \Countable, \Doctrine\Common\Collections\Collection
{
    /**
     * The items collection
     * @var array
     */
    protected $items = array();

    /**
     * The response items collection
     * @var array
     */
    protected $requestItems = array();

    /**
     * The previous request last key
     * @var string|null
     */
    protected $nextContext;

    /**
     * The previous request count
     * @var int|null
     */
    protected $requestCount = 0;

    /**
     * @param Context\Collection|null $nextContext
     * @param int $requestCount The previous request count
     * @param array $requestItems The unparserd request items
     */
    function __construct(Context\Collection $nextContext = null, $requestCount = 0, $requestItems = array())
    {
        $this->nextContext = $nextContext;
        $this->requestCount = $requestCount;
        $this->requestItems = $requestItems;
    }

    /**
     * Return the previous request last key
     * @return null|\Riverline\DynamoDB\Context\Query|\Riverline\DynamoDB\Context\Scan
     */
    public function getNextContext()
    {
        return $this->nextContext;
    }

    /**
     * Return true if the previous request has more items to retreive
     * @return bool
     */
    public function more()
    {
        return !empty($this->nextContext);
    }

    /**
     * Add an item to the collection
     * @param object $item
     */
    public function add($item)
    {
        $this->items[] = $item;
    }

    /**
     * Remove an item off the beginning of the collection
     * @return Item
     */
    public function shift()
    {
        return array_shift($this->items);
    }

    /**
     * Extracts a slice of the collection items
     *
     * @param int $offset If offset is non-negative, the sequence will start at that offset in the array. If offset is negative, the sequence will start that far from the end of the array.
     * @param int|null $length If length is given and is positive, then the sequence will have up to that many elements in it. If the array is shorter than the length, then only the available array elements will be present. If length is given and is negative then the sequence will stop that many elements from the end of the array. If it is omitted, then the sequence will have everything from offset up until the end of the array.
     * @param bool $preserve_keys slice will reorder and reset the numeric array indices by default. You can change this behaviour by setting preserve_keys to TRUE.
     *
     * @return Collection
     */
    public function slice($offset, $length = null, $preserve_keys = false)
    {
        $this->items = array_slice($this->items, $offset, $length, $preserve_keys);
        return $this;
    }

    /**
     * Sort the collection's items by values using user-defined criterias
     *
     * @param array $criteria If offset is non-negative, the sequence will start at that offset in the array. If offset is negative, the sequence will start that far from the end of the array.
     *
     * @return Collection
     */
    public function sort(array $criteria)
    {
        if (!empty($criteria)) {
            $attributes = array_keys($criteria);
            end($attributes);
            do {
                $attribute = current($attributes);
                $direction = strtoupper($criteria[$attribute]);
                $order = function ($a, $b) use ($attribute, $direction) {
                    $p = new \ReflectionProperty(get_class($a), $attribute);
                    $p->setAccessible(true);
                    if ($p->getValue($a) == $p->getValue($b)) {
                        return 0;
                    }
                    $factor = ($direction == "ASC" ? 1 : -1);
                    return ($p->getValue($a) < $p->getValue($b)) ? -$factor : $factor;
                };
                usort($this->items, $order);
            } while (prev($attributes));
        }

        return $this;
    }

    /**
     * Merge a collection with the current collection
     * @param Collection $collection The collection to merge
     */
    public function merge(Collection $collection)
    {
        $this->requestCount += count($collection);
        foreach ($collection as $item) {
            $this->add($item);
        }
    }

    /**
     * Get the collection requestItems
     * @return \ArrayIterator
     */
    public function getRequestItems()
    {
        return $this->requestItems;
    }

    /**
     * Get the collection items
     * @return \array
     */
    public function getItems()
    {
        return $this->items;
    }

    /**
     * @see \IteratorAggregate
     * @return \ArrayIterator
     */
    public function getIterator()
    {
        return new \ArrayIterator($this->items);
    }

    /**
     * @see \Countable
     * @return int
     */
    public function count()
    {
        if (empty($this->items)) {
            // Collection from a count request
            return $this->requestCount;
        } else {
            // Real items count
            return count($this->items);
        }
    }

    /**
     * @return Item
     */
    public function first()
    {
        if (empty($this->items)) {
            return null;
        } else {
            return reset($this->items);
        }
    }

    /**
     * @return Item
     */
    public function last()
    {
        if (empty($this->items)) {
            return null;
        } else {
            return end($this->items);
        }
    }

    /**
     * Clears the collection, removing all elements.
     *
     * @return void
     */
    public function clear()
    {
        $this->items = array();
        $this->requestItems = array();
        $this->requestCount = 0;
    }

    /**
     * Checks whether an element is contained in the collection.
     * This is an O(n) operation, where n is the size of the collection.
     *
     * @param mixed $element The element to search for.
     *
     * @return boolean TRUE if the collection contains the element, FALSE otherwise.
     */
    public function contains($element)
    {
        return in_array($element, $this->items);
    }

    /**
     * Checks whether the collection is empty (contains no elements).
     *
     * @return boolean TRUE if the collection is empty, FALSE otherwise.
     */
    public function isEmpty()
    {
        return count($this->items) == 0;
    }

    /**
     * Removes the element at the specified index from the collection.
     *
     * @param string|integer $key The kex/index of the element to remove.
     *
     * @return mixed The removed element or NULL, if the collection did not contain the element.
     */
    public function remove($key)
    {
        if (isset($this->items[$key])) {
            $elt = $this->items[$key];
            unset($this->items[$key]);
            return $elt;
        }
        return null;
    }

    /**
     * Removes the specified element from the collection, if it is found.
     *
     * @param mixed $element The element to remove.
     *
     * @return boolean TRUE if this collection contained the specified element, FALSE otherwise.
     */
    public function removeElement($element)
    {
        if ($this->contains($element)) {
            unset($this->items[array_search($element, $this->items)]);
            return true;
        }
        return false;
    }

    /**
     * Checks whether the collection contains an element with the specified key/index.
     *
     * @param string|integer $key The key/index to check for.
     *
     * @return boolean TRUE if the collection contains an element with the specified key/index,
     *                 FALSE otherwise.
     */
    public function containsKey($key)
    {
        return isset($this->items[$key]);
    }

    /**
     * Gets the element at the specified key/index.
     *
     * @param string|integer $key The key/index of the element to retrieve.
     *
     * @return mixed
     */
    public function get($key)
    {
        return $this->items[$key];
    }

    /**
     * Gets all keys/indices of the collection.
     *
     * @return array The keys/indices of the collection, in the order of the corresponding
     *               elements in the collection.
     */
    public function getKeys()
    {
        return array_keys($this->items);
    }

    /**
     * Gets all values of the collection.
     *
     * @return array The values of all elements in the collection, in the order they
     *               appear in the collection.
     */
    public function getValues()
    {
        return $this->items;
    }

    /**
     * Sets an element in the collection at the specified key/index.
     *
     * @param string|integer $key The key/index of the element to set.
     * @param mixed $value The element to set.
     *
     * @return void
     */
    public function set($key, $value)
    {
        $this->items[$key] = $value;
    }

    /**
     * Gets a native PHP array representation of the collection.
     *
     * @return array
     */
    public function toArray()
    {
        return $this->items;
    }

    /**
     * Gets the key/index of the element at the current iterator position.
     *
     * @return int|string
     */
    public function key()
    {
        return key($this->items);
    }

    /**
     * Gets the element of the collection at the current iterator position.
     *
     * @return mixed
     */
    public function current()
    {
        return current($this->items);
    }

    /**
     * Moves the internal iterator position to the next element and returns this element.
     *
     * @return mixed
     */
    public function next()
    {
        return next($this->items);
    }

    /**
     * Tests for the existence of an element that satisfies the given predicate.
     *
     * @param Closure $p The predicate.
     *
     * @return boolean TRUE if the predicate is TRUE for at least one element, FALSE otherwise.
     */
    public function exists(Closure $p)
    {
        // TODO: Implement exists() method.
    }

    /**
     * Returns all the elements of this collection that satisfy the predicate p.
     * The order of the elements is preserved.
     *
     * @param Closure $p The predicate used for filtering.
     *
     * @return \Doctrine\Common\Collections\Collection A collection with the results of the filter operation.
     */
    public function filter(Closure $p)
    {
        return array_filter($this->items, $p);
    }

    /**
     * Tests whether the given predicate p holds for all elements of this collection.
     *
     * @param Closure $p The predicate.
     *
     * @return boolean TRUE, if the predicate yields TRUE for all elements, FALSE otherwise.
     */
    public function forAll(Closure $p)
    {
        // TODO: Implement forAll() method.
    }

    /**
     * Applies the given function to each element in the collection and returns
     * a new collection with the elements returned by the function.
     *
     * @param Closure $func
     *
     * @return \Doctrine\Common\Collections\Collection
     */
    public function map(Closure $func)
    {
        return array_map($func, $this->items);
    }

    /**
     * Partitions this collection in two collections according to a predicate.
     * Keys are preserved in the resulting collections.
     *
     * @param Closure $p The predicate on which to partition.
     *
     * @return array An array with two elements. The first element contains the collection
     *               of elements where the predicate returned TRUE, the second element
     *               contains the collection of elements where the predicate returned FALSE.
     */
    public function partition(Closure $p)
    {
        // TODO: Implement partition() method.
    }

    /**
     * Gets the index/key of a given element. The comparison of two elements is strict,
     * that means not only the value but also the type must match.
     * For objects this means reference equality.
     *
     * @param mixed $element The element to search for.
     *
     * @return int|string|bool The key/index of the element or FALSE if the element was not found.
     */
    public function indexOf($element)
    {
        return array_search($element, $this->items);
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Whether a offset exists
     * @link http://php.net/manual/en/arrayaccess.offsetexists.php
     * @param mixed $offset <p>
     * An offset to check for.
     * </p>
     * @return boolean true on success or false on failure.
     * </p>
     * <p>
     * The return value will be casted to boolean if non-boolean was returned.
     */
    public function offsetExists($offset)
    {
        return isset($this->items[$offset]);
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Offset to retrieve
     * @link http://php.net/manual/en/arrayaccess.offsetget.php
     * @param mixed $offset <p>
     * The offset to retrieve.
     * </p>
     * @return mixed Can return all value types.
     */
    public function offsetGet($offset)
    {
        return $this->items[$offset];
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Offset to set
     * @link http://php.net/manual/en/arrayaccess.offsetset.php
     * @param mixed $offset <p>
     * The offset to assign the value to.
     * </p>
     * @param mixed $value <p>
     * The value to set.
     * </p>
     * @return void
     */
    public function offsetSet($offset, $value)
    {
        $this->items[$offset] = $value;
    }

    /**
     * (PHP 5 &gt;= 5.0.0)<br/>
     * Offset to unset
     * @link http://php.net/manual/en/arrayaccess.offsetunset.php
     * @param mixed $offset <p>
     * The offset to unset.
     * </p>
     * @return void
     */
    public function offsetUnset($offset)
    {
        unset($this->items[$offset]);
    }
}