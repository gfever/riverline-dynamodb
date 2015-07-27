<?php

namespace Riverline\DynamoDB;

/**
 * @class
 */
class Collection implements \IteratorAggregate, \Countable
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
        if(!empty($criteria)) {
            $attributes = array_keys($criteria);
            end($attributes);
            do {
                $attribute = current($attributes);
                $direction = strtoupper($criteria[$attribute]);
                $order = function ($a, $b) use ($attribute, $direction) {
                    if ($a[$attribute] == $b[$attribute]) {
                        return 0;
                    }
                    $factor = ($direction == "ASC" ? 1 : -1);
                    return ($a[$attribute] < $b[$attribute]) ? -$factor : $factor;
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
}