<?php

namespace Riverline\DynamoDB\Context;

use \Riverline\DynamoDB\AttributeCondition;
use Satoripop\DynamoDBBundle\Util\EntityRepository;

class Scan extends Collection
{
    /**
     * @var EntityRepository $repo
     */
    private $repo;

    /**
     * @param EntityRepository $repo
     */
    public function __construct(EntityRepository $repo = null)
    {
        $this->repo = $repo;
    }

    /**
     * @var array
     */
    protected $filters = array();

    /**
     * @param string $name
     * @param string $operator
     * @param mixed $value
     * @return \Riverline\DynamoDB\Context\Scan
     */
    public function addFilter($name, $operator, $value)
    {
        $this->filters[$name] = new AttributeCondition($operator, $value);

        return $this;
    }

    /**
     * @param bool $ConsistentRead
     * @throws \Riverline\DynamoDB\Exception\AttributesException
     */
    public function setConsistentRead($ConsistentRead)
    {
        throw new \Riverline\DynamoDB\Exception\AttributesException('Scan do not support consistent read');
    }

    /**
     * Return the context formated for DynamoDB
     * @return array
     */
    public function getForDynamoDB()
    {
        $parameters = parent::getForDynamoDB();

        foreach ($this->filters as $name => $filter) {
            /* @var $filter AttributeCondition */
            $parameters['ScanFilter'][$name] = $filter->getForDynamoDB();
        }

        return $parameters;
    }

    /**
     * @return EntityRepository
     */
    public function getRepo()
    {
        return $this->repo;
    }

    /**
     * @param EntityRepository $repo
     */
    public function setRepo(EntityRepository $repo)
    {
        $this->repo = $repo;
    }
}