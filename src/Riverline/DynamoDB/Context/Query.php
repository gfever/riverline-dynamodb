<?php

namespace Riverline\DynamoDB\Context;

use \Riverline\DynamoDB\AttributeCondition;
use Satoripop\DynamoDBBundle\Util\EntityRepository;

class Query extends Collection
{
    /**
     * @var AttributeCondition
     */
    protected $rangeCondition;

    /**
     * @var boolean
     */
    protected $scanIndexForward;

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
     * @param string $operator
     * @param mixed $attributes
     * @return \Riverline\DynamoDB\Context\Query
     */
    public function setRangeCondition($operator, $attributes)
    {
        $this->rangeCondition = new AttributeCondition($operator, $attributes);

        return $this;
    }

    /**
     * @param boolean $ScanIndexForward
     * @return \Riverline\DynamoDB\Context\Query
     */
    public function setScanIndexForward($ScanIndexForward)
    {
        $this->scanIndexForward = (bool)$ScanIndexForward;

        return $this;
    }

    /**
     * Return the context formated for DynamoDB
     * @return array
     */
    public function getForDynamoDB()
    {
        $parameters = parent::getForDynamoDB();

        $rangeCondition = $this->rangeCondition;
        if (null !== $rangeCondition) {
            $parameters['RangeKeyCondition'] = $rangeCondition->getForDynamoDB();
        }

        $scanIndexForward = $this->scanIndexForward;
        if (null !== $scanIndexForward) {
            $parameters['ScanIndexForward'] = $scanIndexForward;
        }

        return $parameters;
    }

    /**
     * @static
     * @param string $operator
     * @param mixed $attributes
     * @return \Riverline\DynamoDB\Context\Query
     */
    static public function create($operator, $attributes)
    {
        $query = new Query();
        $query->setRangeCondition($operator, $attributes);

        return $query;
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
    public function setRepo($repo)
    {
        $this->repo = $repo;
    }
}