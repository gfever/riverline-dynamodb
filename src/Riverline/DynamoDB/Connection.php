<?php

namespace Riverline\DynamoDB;

use Doctrine\ORM\Mapping\ClassMetadata;
use Satoripop\DynamoDBBundle\Util\DynamoPlatform;
use Riverline\DynamoDB\Logger\Logger;

use Aws\DynamoDb\DynamoDbClient;
use Satoripop\DynamoDBBundle\Util\ParamConverter;
use Satoripop\DynamoDBBundle\Util\UnitOfWork;

/**
 * @class
 */
class Connection
{
    /**
     * @var \Aws\DynamoDb\DynamoDbClient
     */
    protected $connector;

    /**
     * @var \Riverline\DynamoDB\Logger\Logger
     */
    protected $logger;

    /**
     * Read and Write unit counter
     * @var int
     */
    protected $readUnit = array(), $writeUnit = array();

    /**
     * @var ClassMetadata
     *
     * @api
     */
    protected $metadata;

    /**
     * @var UnitOfWork
     *
     * @api
     */
    protected $uow;

    /**
     * @param string $key The AWS access Key
     * @param string $secret The AWS secret Key
     * @param string $region The DynamoDB region endpoint
     * @throws \RuntimeException
     */
    public function __construct($key, $secret, $region)
    {
        if (!class_exists('Aws\DynamoDb\DynamoDbClient')) {
            throw new \RuntimeException('Missing AWS PHP SDK');
        }

        $this->connector = DynamoDbClient::factory(array(
            'key' => $key,
            'secret' => $secret,
            'region' => $region,
            'version' => '2011-12-05'
        ));
    }

    public function fetchColumn($value)
    {
        return $value;
    }

    /**
     * @param ClassMetadata $metadata
     * @return $this
     */
    public function setMetadata(ClassMetadata $metadata)
    {
        $this->metadata = $metadata;
        return $this;
    }

    /**
     * @return ClassMetadata
     */
    public function getMetadata()
    {
        return $this->metadata;
    }

    public function getDatabasePlatform()
    {
        return new DynamoPlatform();
    }

    /**
     * Set a logger for this connection
     * @param Logger $logger
     */
    public function setLogger(Logger $logger)
    {
        $this->logger = $logger;

        $this->log('Logger activated');
    }

    /**
     * Log a message via the logger
     * @param string $message
     * @param int $level
     */
    protected function log($message, $level = Logger::INFO)
    {
        $this->logger->log($message, $level);
    }

    /**
     * Return the DynamoDB object
     * @return \Aws\DynamoDb\DynamoDbClient
     */
    public function getConnector()
    {
        return $this->connector;
    }

    /**
     * Return the number of read units consumed
     * @param string|null $table If null, return consumed units for all tables
     * @return float
     */
    public function getConsumedReadUnits($table = null)
    {
        if (is_null($table)) {
            return array_sum($this->readUnit);
        } else {
            return (isset($this->readUnit[$table]) ? $this->readUnit[$table] : 0);
        }
    }

    /**
     * Update the Read Units counter
     * @param string $table
     * @param float $units
     */
    protected function addConsumedReadUnits($table, $units)
    {
        if (null !== $this->logger) {
            $this->log($units . ' consumed read units on table ' . $table);
        }

        if (isset($this->readUnit[$table])) {
            $this->readUnit[$table] += $units;
        } else {
            $this->readUnit[$table] = $units;
        }
    }

    /**
     * Return the number of write units consumed
     * @param string|null $table If null, return consumed units for all tables
     * @return float
     */
    public function getConsumedWriteUnits($table = null)
    {
        if (is_null($table)) {
            return array_sum($this->writeUnit);
        } else {
            return (isset($this->writeUnit[$table]) ? $this->writeUnit[$table] : 0);
        }
    }

    /**
     * Update the Write Units counter
     * @param string $table
     * @param float $units
     */
    protected function addConsumedWriteUnits($table, $units)
    {
        if (null !== $this->logger) {
            $this->log($units . ' consumed write units on table ' . $table);
        }

        if (isset($this->writeUnit[$table])) {
            $this->writeUnit[$table] += $units;
        } else {
            $this->writeUnit[$table] = $units;
        }
    }

    /**
     * Reset the read and write unit counter
     * @param string|null $table If null, reset all consumed units
     */
    public function resetConsumedUnits($table = null)
    {
        if (is_null($table)) {
            if (null !== $this->logger) {
                $this->log('Reset all consumed units counters');
            }
            $this->readUnit = array();
            $this->writeUnit = array();
        } else {
            if (null !== $this->logger) {
                $this->log('Reset consumed units counters for table ' . $table);
            }
            unset(
                $this->readUnit[$table],
                $this->writeUnit[$table]
            );
        }
    }

    /**
     * Add an item to DynamoDB via the put_item call
     * @param Item $item
     * @param Context\Put|null $context The call context
     * @return array|null
     * @throws Exception\AttributesException
     */
    public function put(Item $item, Context\Put $context = null)
    {
        $table = $item->getTable();

        if (null !== $this->logger) {
            $this->log('Put on table ' . $table);
        }

        if (empty($table)) {
            throw new \Riverline\DynamoDB\Exception\AttributesException('Item do not have table defined');
        }

        $attributes = array();
        foreach ($item as $name => $attribute) {
            /** @var $attribute \Riverline\DynamoDB\Attribute */
            if ("" !== $attribute->getValue()) {
                // Only not empty string
                $attributes[$name] = $attribute->getForDynamoDB();
            }
        }
        $parameters = array(
            'TableName' => $table,
            'Item' => $attributes,
        );

        if (null !== $context) {
            $parameters += $context->getForDynamoDB();
        }

        if (null !== $this->logger) {
            $this->log('Put request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->putItem($parameters);

        if (null !== $this->logger) {
            $this->log('Put request response : ' . print_r($response, true), Logger::DEBUG);
        }

        // Update write counter
        $this->addConsumedWriteUnits($table, floatval($response['ConsumedCapacityUnits']));

        return $this->populateAttributes($response);
    }

    /**
     * Delete an item via the delete_item call
     * @param string $table The item table
     * @param mixed $hash The primary hash key
     * @param mixed|null $range The primary range key
     * @param Context\Delete|null $context The call context
     * @return array|null
     */
    public function delete($table, $hash, $range = null, Context\Delete $context = null)
    {
        if (null !== $this->logger) {
            $this->log('Delete on table ' . $table);
        }

        // Primary key
        $hash = new Attribute($hash);
        $key = array(
            'HashKeyElement' => $hash->getForDynamoDB()
        );

        // Range key
        if (null !== $range) {
            $range = new Attribute($range);
            $key['RangeKeyElement'] = $range->getForDynamoDB();
        }

        $parameters = array(
            'TableName' => $table,
            'Key' => $key
        );

        if (null !== $context) {
            $parameters += $context->getForDynamoDB();
        }

        if (null !== $this->logger) {
            $this->log('Delete request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->deleteItem($parameters);

        if (null !== $this->logger) {
            $this->log('Delete request response : ' . print_r($response, true), Logger::DEBUG);
        }

        // Update write counter
        $this->addConsumedWriteUnits($table, floatval($response['ConsumedCapacityUnits']));

        return $this->populateAttributes($response);
    }

    /**
     * Get an item via the get_item call
     * @param string $table The item table
     * @param mixed $hash The primary hash key
     * @param mixed|null $range The primary range key
     * @param Context\Get|null $context The call context
     * @return Item|null
     */
    public function get($table, $entity, $hash, $range = null, Context\Get $context = null)
    {
        if (null !== $this->logger) {
            $this->log('Get on table ' . $table);
        }

        // Primary key
        $hash = new Attribute($hash);

        $parameters = array(
            'TableName' => $table,
            'Key' => array(
                'HashKeyElement' => $hash->getForDynamoDB()
            )
        );

        // Range key
        if (null !== $range) {
            $range = new Attribute($range);
            $parameters['Key']['RangeKeyElement'] = $range->getForDynamoDB();
        }

        if (null !== $context) {
            $parameters += $context->getForDynamoDB();
        }

        if (null !== $this->logger) {
            $this->log('Get request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->getItem($parameters);

        if (null !== $this->logger) {
            $this->log('Get request response : ' . print_r($response, true), Logger::DEBUG);
        }

        $this->addConsumedReadUnits($table, floatval($response['ConsumedCapacityUnits']));

        if (isset($response['Item'])) {
            $item = new $entity();
            $this->populateFromDynamoDB($item, $response['Item']);
            return $item;
        } else {
            if (null !== $this->logger) {
                $this->log('Didn\'t find item');
            }
            return null;
        }
    }

    /**
     * Update an item via the update_item call
     * @param string $table The item table
     * @param mixed $hash The primary hash key
     * @param mixed|null $range The primary range key
     * @param AttributeUpdate $update
     * @param Context\Update|null $context The call context
     * @return array|null
     * @throws Exception\AttributesException
     */
    public function update($table, $hash, $range = null, AttributeUpdate $update, Context\Update $context = null)
    {
        if (null !== $this->logger) {
            $this->log('Update on table' . $table);
        }

        // Primary key
        $hash = new Attribute($hash);
        $key = array(
            'HashKeyElement' => $hash->getForDynamoDB()
        );

        // Range key
        if (null !== $range) {
            $range = new Attribute($range);
            $key['RangeKeyElement'] = $range->getForDynamoDB();
        }

        $attributes = array();
        foreach ($update as $name => $attribute) {
            /** @var $attribute Attribute */
            $attributes[$name] = $attribute->getForDynamoDB();
        }

        $parameters = array(
            'TableName' => $table,
            'Key' => $key,
            'AttributeUpdates' => $attributes,
        );

        if (null !== $context) {
            $parameters += $context->getForDynamoDB();
        }

        if (null !== $this->logger) {
            $this->log('Update request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->updateItem($parameters);

        if (null !== $this->logger) {
            $this->log('Update request response : ' . print_r($response, true), Logger::DEBUG);
        }

        // Update write counter
        $this->addConsumedWriteUnits($table, floatval($response['ConsumedCapacityUnits']));

        return $this->populateAttributes($response);
    }

    /**
     * Get items via the query call
     * @param string $table The item table
     * @param mixed $hash The primary hash key
     * @param Context\Query|null $context The call context
     * @return Collection
     */
    public function query($table, $hash, Context\Query $context = null)
    {
        if (null !== $this->logger) {
            $this->log('Query on table ' . $table);
        }

        $hash = new Attribute($hash);
        $parameters = array(
            'TableName' => $table,
            'HashKeyValue' => $hash->getForDynamoDB(),
        );

        if (null !== $context) {
            $parameters += $context->getForDynamoDB();
        }

        if (null !== $this->logger) {
            $this->log('Query request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->query($parameters);

        if (null !== $this->logger) {
            $this->log('Query request response : ' . print_r($response, true), Logger::DEBUG);
        }

        $this->addConsumedReadUnits($table, floatval($response['ConsumedCapacityUnits']));

        if (isset($response['LastEvaluatedKey'])) {
            if (null === $context) {
                $nextContext = new Context\Query();
            } else {
                $nextContext = clone $context;
            }
            $nextContext->setExclusiveStartKey($response['LastEvaluatedKey']);

            if (null !== $this->logger) {
                $this->log('More Items to retrieve');
            }
        } else {
            $nextContext = null;
        }

        $items = new Collection(
            $nextContext,
            $response['Count']
        );
        if (!empty($response['Items'])) {
            foreach ($response['Items'] as $responseItem) {
                $item = new Item($table);
                $item->populateFromDynamoDB($responseItem);
                $items->add($item);
            }
        }

        if (null !== $this->logger) {
            $this->log('Find ' . count($items) . ' Items');
        }

        return $items;
    }

    /**
     * Get items via the scan call
     * @param string $table The item table
     * @param string $entity The item class
     * @param Context\Scan|null $context The call context
     * @return Collection
     */
    public function scan($table, $entity, Context\Scan $context = null)
    {
        $response = $this->getScanResponse($table, $context);

        $items = $this->getScanCollection($response, $context);

        return $this->populateItems($items, $entity);
    }

    /**
     * Get the scan call's response
     * @param Collection $items The item collection
     * @param string $entity The item class
     * @return Collection
     */
    public function populateItems($items, $entity)
    {
        if (!empty($requestItems = $items->getRequestItems())) {
            foreach ($requestItems as $responseItem) {
                $item = new $entity();
                $this->populateFromDynamoDB($item, $responseItem);
                $items->add($item);
            }
        }

        if (null !== $this->logger) {
            $this->log('Find ' . count($items) . ' Items');
        }

        return $items;
    }

    public function populateFromDynamoDB(&$item, array $responseItem)
    {
        $metas = clone $this->getMetadata();
        $cnv = new ParamConverter($metas, $this->getUow());
        $response = $responseItem;
        foreach ($response as $name => $value) {
            list ($type, $value) = each($value);
            try {
                if (in_array($name, $metas->getFieldNames()) || in_array($name, array_keys($metas->getAssociationMappings()))) {
                    $r = new \ReflectionProperty(get_class($item), $name);
                    $r->setAccessible(true);
                    $r->setValue($item, $cnv->transform($name, $value));
                }
            } catch (\Exception $e) {
                var_dump($e->getMessage()); die;
            }
        }
        return $item;
    }

    /**
     * @return UnitOfWork
     */
    public function getUow()
    {
        return $this->uow;
    }

    /**
     * @param UnitOfWork $uow
     */
    public function setUow($uow)
    {
        $this->uow = $uow;
    }

    private function humanize($str)
    {
        return str_replace(' ', '', ucwords(trim(str_replace('_', ' ', $str))));
    }

    /**
     * Creates a collection from a call's response
     * @param array $response The call's response
     * @param Context\Scan|null $context The call context
     * @return Collection
     */
    public function getScanCollection($response, Context\Scan $context = null)
    {
        if (isset($response['LastEvaluatedKey'])) {
            if (null === $context) {
                $nextContext = new Context\Scan();
            } else {
                $nextContext = clone $context;
            }
            $nextContext->setExclusiveStartKey($response['LastEvaluatedKey']);

            if (null !== $this->logger) {
                $this->log('More Items to retrieve');
            }
        } else {
            $nextContext = null;
        }

        return new Collection(
            $nextContext,
            $response['Count'],
            $response['Items']
        );

    }

    /**
     * Get the scan call's response
     * @param string $table The item table
     * @param Context\Scan|null $context The call context
     * @return array
     */
    public function getScanResponse($table, Context\Scan $context = null)
    {
        if (null !== $this->logger) {
            $this->log('Scan on table ' . $table);
        }

        $parameters = array(
            'TableName' => $table
        );

        if (null !== $context) {
            $parameters += $context->getForDynamoDB();
        }

        if (null !== $this->logger) {
            $this->log('Scan request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->scan($parameters);

        if (null !== $this->logger) {
            $this->log('Scan request response : ' . print_r($response, true), Logger::DEBUG);
            $this->log($response['ScannedCount'] . ' scanned items');
        }

        $this->addConsumedReadUnits($table, floatval($response['ConsumedCapacityUnits']));

        return $response;
    }

    /**
     * Get a batch of items
     * @param Context\BatchGet $context
     * @throws \Riverline\DynamoDB\Exception\AttributesException
     * @return \Riverline\DynamoDB\Batch\BatchCollection
     */
    public function batchGet(Context\BatchGet $context)
    {
        if (null !== $this->logger) {
            $this->log('BatchGet');
        }

        if (0 === count($context)) {
            $message = "BatchGet context doesn't contain any key to get";
            if (null !== $this->logger) {
                $this->log($message, Logger::ERROR);
            }
            throw new Exception\AttributesException($message);
        }

        $parameters = $context->getForDynamoDB();

        if (null !== $this->logger) {
            $this->log('BatchGet request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->batchGetItem($parameters);

        if (null !== $this->logger) {
            $this->log('BatchGet request response : ' . print_r($response, true), Logger::DEBUG);
        }

        // UnprocessedKeys
        if (count((array)$response['UnprocessedKeys'])) {
            $unprocessKeyContext = new Context\BatchGet();
            foreach ($response['UnprocessedKeys'] as $table => $tableParameters) {
                foreach ($tableParameters->Keys as $key) {
                    $unprocessKeyContext->addKey($table, current($key->HashKeyElement), current($key->RangeKeyElement));
                }
                if (!empty($tableParameters->AttributesToGet)) {
                    $unprocessKeyContext->setAttributesToGet($table, $tableParameters->AttributesToGet);
                }
            }
        } else {
            $unprocessKeyContext = null;
        }

        $collection = new Batch\BatchCollection($unprocessKeyContext);

        foreach ($response['Responses'] as $table => $responseItems) {
            $this->addConsumedReadUnits($table, floatval($responseItems['ConsumedCapacityUnits']));

            $items = new Collection();
            foreach ($responseItems['Items'] as $responseItem) {
                $item = new Item($table);
                $item->populateFromDynamoDB($responseItem);
                $items->add($item);
            }

            if (null !== $this->logger) {
                $this->log('Find ' . count($items) . ' Items on table ' . $table);
            }

            $collection->setItems($table, $items);
        }

        return $collection;
    }

    /**
     * Put Items and delete Keys by batch
     * @param Context\BatchWrite $context
     * @return null|Context\BatchWrite Return a new BatchWrite context if some request were not processed
     * @throws Exception\AttributesException
     */
    public function batchWrite(Context\BatchWrite $context)
    {
        if (null !== $this->logger) {
            $this->log('BatchWrite');
        }

        if (0 === count($context)) {
            $message = "BatchWrite context doesn't contain anything to write";
            if (null !== $this->logger) {
                $this->log($message, Logger::ERROR);
            }
            throw new Exception\AttributesException($message);
        }

        $parameters = $context->getForDynamoDB();

        if (null !== $this->logger) {
            $this->log('BatchWrite request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->batchWriteItem($parameters);

        if (null !== $this->logger) {
            $this->log('BatchWrite request response : ' . print_r($response, true), Logger::DEBUG);
        }

        // UnprocessedKeys
        if (count((array)$response['UnprocessedItems'])) {
            $newContext = new Context\BatchWrite();
            foreach ($response['UnprocessedItems'] as $table => $tableParameters) {
                foreach ($tableParameters as $request) {
                    if (isset($request['DeleteRequest'])) {
                        $keys = $request['DeleteRequest']['Key'];
                        $newContext->addKeyToDelete(
                            $table,
                            current($keys['HashKeyElement']),
                            (isset($keys['RangeKeyElement']) ? current($keys['RangeKeyElement']) : null)
                        );
                    } elseif (isset($request['PutRequest'])) {
                        $item = new Item($table);
                        $item->populateFromDynamoDB($request['PutRequest']['Item']);
                        $newContext->addItemToPut($item);
                    }
                }
            }

            if (null !== $this->logger) {
                $this->log('More unprocessed Items');
            }
        } else {
            $newContext = null;
        }

        // Write Unit
        foreach ($response['Responses'] as $table => $responseItems) {
            $this->addConsumedWriteUnits($table, floatval($responseItems['ConsumedCapacityUnits']));
        }

        return $newContext;
    }

    /**
     * Create table via the create_table call
     * @param string $table The name of the table
     * @param Table\KeySchema $keySchama
     * @param Table\ProvisionedThroughput $provisionedThroughput
     * @return Table\TableDescription
     */
    public function createTable($table, Table\KeySchema $keySchama, Table\ProvisionedThroughput $provisionedThroughput)
    {
        if (null !== $this->logger) {
            $this->log('Create table ' . $table);
        }

        $parameters = array(
            'TableName' => $table,
            'KeySchema' => $keySchama->getForDynamoDB(),
            'ProvisionedThroughput' => $provisionedThroughput->getForDynamoDB()
        );

        if (null !== $this->logger) {
            $this->log('TableCreate request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->createTable($parameters);

        if (null !== $this->logger) {
            $this->log('TableCreate request response : ' . print_r($response, true), Logger::DEBUG);
        }
    }

    /**
     * Update table via the update_table call
     * @param string $table The name of the table
     * @param Table\ProvisionedThroughput $provisionedThroughput
     * @return Table\TableDescription
     */
    public function updateTable($table, Table\ProvisionedThroughput $provisionedThroughput)
    {
        if (null !== $this->logger) {
            $this->log('Update table ' . $table);
        }

        $parameters = array(
            'TableName' => $table,
            'ProvisionedThroughput' => $provisionedThroughput->getForDynamoDB()
        );

        if (null !== $this->logger) {
            $this->log('UpdateTable request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->updateTable($parameters);

        if (null !== $this->logger) {
            $this->log('UpdateTable request response : ' . print_r($response, true), Logger::DEBUG);
        }
    }

    /**
     * Delete table via the delete_table call
     * @param string $table The name of the table
     * @return Table\TableDescription
     */
    public function deleteTable($table)
    {
        if (null !== $this->logger) {
            $this->log('Delete table ' . $table);
        }

        $parameters = array(
            'TableName' => $table,
        );

        if (null !== $this->logger) {
            $this->log('DeleteTable request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->deleteTable($parameters);

        if (null !== $this->logger) {
            $this->log('DeleteTable request response : ' . print_r($response, true), Logger::DEBUG);
        }
    }

    /**
     * Describe table via the describe_table call
     * @param string $table The name of the table
     * @return Table\TableDescription
     */
    public function describeTable($table)
    {
        if (null !== $this->logger) {
            $this->log('Describe table ' . $table);
        }

        $parameters = array(
            'TableName' => $table,
        );

        if (null !== $this->logger) {
            $this->log('DescribeTable request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->describeTable($parameters);

        if (null !== $this->logger) {
            $this->log('DescribeTable request response : ' . print_r($response, true), Logger::DEBUG);
        }

        $tableDescription = new Table\TableDescription();
        $tableDescription->populateFromDynamoDB($response['Table']);

        return $tableDescription;
    }

    /**
     * List tables via the list_tables call
     * @param integer $limit
     * @param string $exclusiveStartTableName
     * @return Table\TableCollection
     */
    public function listTables($limit = null, $exclusiveStartTableName = null)
    {
        if (null !== $this->logger) {
            $this->log('List tables');
        }

        $parameters = array();
        if (null !== $limit) {
            $parameters['Limit'] = $limit;
        }
        if (null !== $exclusiveStartTableName) {
            $parameters['ExclusiveStartTableName'] = $exclusiveStartTableName;
        }

        if (null !== $this->logger) {
            $this->log('ListTable request paramaters : ' . print_r($parameters, true), Logger::DEBUG);
        }

        $response = $this->connector->listTables($parameters);

        if (null !== $this->logger) {
            $this->log('ListTable request response : ' . print_r($response, true), Logger::DEBUG);
        }

        $tables = new Table\TableCollection((isset($response['LastEvaluatedTableName']) ? $response['LastEvaluatedTableName'] : null));
        if (!empty($response['TableNames'])) {
            foreach ($response['TableNames'] as $table) {
                $tables->add($table);
            }
        }
        return $tables;
    }

    public function waitForTableToBeInState($table, $status, $sleep = 3, $max = 20)
    {
        $current = 0;
        do {
            $tableDescription = $this->describeTable($table);
            if ($status == $tableDescription->getTableStatus()) {
                return $tableDescription;
            } else {
                if (null !== $this->logger) {
                    $this->log('Table status is ' . $tableDescription->getTableStatus() . ', waiting');
                }
                sleep($sleep);
            }
        } while (++$current < $max);

        if (null !== $this->logger) {
            $this->log('Timeout while waiting for table to be in state', Logger::ERROR);
        }

        throw new \Exception('waitForTableToBeInState timeout');
    }

    /**
     * Extract the attributes array from response data
     * @param \Guzzle\Service\Resource\Model $data The response body data
     * @return array|null
     */
    protected function populateAttributes(\Guzzle\Service\Resource\Model $data)
    {
        if (isset($data['Attributes'])) {
            $attributes = array();
            foreach ($data['Attributes'] as $name => $value) {
                list ($type, $value) = each($value);
                $attributes[$name] = new Attribute($value, $type);
            }
            return $attributes;
        } else {
            return null;
        }
    }
}