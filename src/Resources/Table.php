<?php
namespace DreamFactory\Core\Cassandra\Resources;

use DreamFactory\Core\Cassandra\Database\CassandraConnection;
use DreamFactory\Core\Cassandra\Database\Schema\Schema as CasSchema;
use DreamFactory\Core\Contracts\RequestHandlerInterface;
use DreamFactory\Core\Enums\DbResourceTypes;
use DreamFactory\Core\Resources\BaseNoSqlDbTableResource;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Cassandra\Database\Query\CassandraBuilder;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use Illuminate\Database\Query\Builder;
use DreamFactory\Core\Database\Expression;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\ForbiddenException;
use DreamFactory\Core\Utility\DataFormatter;
use DreamFactory\Library\Utility\Enums\Verbs;
use DreamFactory\Core\Utility\ResourcesWrapper;
use Config;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Library\Utility\Scalar;
use DB;
use DreamFactory\Core\Enums\DbSimpleTypes;

class Table extends BaseNoSqlDbTableResource
{
    /** @var  CassandraConnection */
    protected $connection = null;

    /** @var CasSchema */
    protected $schema = null;

    public function setParent(RequestHandlerInterface $parent)
    {
        parent::setParent($parent);

        $this->connection = $this->parent->getConnection();
        $this->schema = $this->parent->getSchema();
    }

    /**
     * @param $table_name
     *
     * @return ColumnSchema[]
     * @throws \Exception
     */
    protected function getFieldsInfo($table_name)
    {
        $table = $this->schema->getResource(DbResourceTypes::TYPE_TABLE, $table_name);
        if (!$table) {
            throw new NotFoundException("Table '$table_name' does not exist in the database.");
        }

        return $table->getColumns(true);
    }

    /**
     * @param      $table
     * @param null $fields_info
     * @param null $requested_fields
     * @param null $requested_types
     *
     * @return array|\DreamFactory\Core\Database\Schema\ColumnSchema[]
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     */
    protected function getIdsInfo($table, $fields_info = null, &$requested_fields = null, $requested_types = null)
    {
        $idsInfo = [];
        if (empty($requested_fields)) {
            $requested_fields = [];
            /** @type ColumnSchema[] $idsInfo */
            $idsInfo = static::getPrimaryKeys($fields_info);
            foreach ($idsInfo as $info) {
                $requested_fields[] = $info->getName(true);
            }
        } else {
            if (false !== $requested_fields = static::validateAsArray($requested_fields, ',')) {
                foreach ($requested_fields as $field) {
                    $ndx = strtolower($field);
                    if (isset($fields_info[$ndx])) {
                        $idsInfo[] = $fields_info[$ndx];
                    }
                }
            }
        }

        return $idsInfo;
    }

    /**
     * @param array   $avail_fields
     * @param boolean $names_only Return only an array of names, otherwise return all properties
     *
     * @return array
     */
    public static function getPrimaryKeys($avail_fields, $names_only = false)
    {
        $keys = [];
        foreach ($avail_fields as $info) {
            if ($info->isPrimaryKey) {
                $keys[] = ($names_only ? $info->name : $info);
            }
        }

        return $keys;
    }

    public function rollbackTransaction()
    {
        // TODO: Implement rollbackTransaction() method.
    }

    /**
     * {@inheritdoc}
     */
    protected function addToTransaction(
        $record = null,
        $id = null,
        $extras = null,
        $rollback = false,
        $continue = false,
        $single = false
    ){
        if ($rollback) {
            // sql transaction really only for rollback scenario, not batching
            if (0 >= $this->connection->transactionLevel()) {
                $this->connection->beginTransaction();
            }
        }

        $ssFilters = array_get($extras, 'ss_filters');
        $updates = array_get($extras, 'updates');
        $idFields = array_get($extras, 'id_fields');
        $needToIterate = ($single || !$continue || (1 < count($this->tableIdsInfo)));

        $related = array_get($extras, 'related');
        $requireMore = Scalar::boolval(array_get($extras, 'require_more')) || !empty($related);
        //$allowRelatedDelete = Scalar::boolval(array_get($extras, 'allow_related_delete'));
        //$relatedInfo = $this->describeTableRelated($this->transactionTable);

        $builder = $this->connection->table($this->transactionTable);
        if (!empty($id)) {
            if (is_array($id)) {
                foreach ($idFields as $name) {
                    $builder->where($name, array_get($id, $name));
                }
            } else {
                $name = array_get($idFields, 0);
                $builder->where($name, $id);
            }
        }

        $fields = array_get($extras, ApiOptions::FIELDS);
        $fields = (empty($fields)) ? $idFields : $fields;

        $serverFilter = $this->buildQueryStringFromData($ssFilters);
        if (!empty($serverFilter)) {
            Session::replaceLookups($serverFilter);
            $params = [];
            $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
            $builder->whereRaw($filterString, $params);
        }

        $out = [];
        switch ($this->getAction()) {
            case Verbs::POST:
                // need the id back in the record
                if (!empty($id)) {
                    if (is_array($id)) {
                        $record = array_merge($record, $id);
                    } else {
                        $record[array_get($idFields, 0)] = $id;
                    }
                }

                //if (!empty($relatedInfo)) {
                //    $this->updatePreRelations($record, $relatedInfo);
                //}

                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                if (empty($id) && (1 === count($this->tableIdsInfo)) && $this->tableIdsInfo[0]->autoIncrement) {
                    $idName = $this->tableIdsInfo[0]->name;
                    $id[$idName] = $builder->insertGetId($parsed, $idName);
                    $record[$idName] = $id[$idName];
                } else {
                    if (!$builder->insert($parsed)) {
                        throw new InternalServerErrorException("Record insert failed.");
                    }
                }

//                if (!empty($relatedInfo)) {
//                    $this->updatePostRelations(
//                        $this->transactionTable,
//                        $record,
//                        $relatedInfo,
//                        $allowRelatedDelete
//                    );
//                }

                $idName =
                    (isset($this->tableIdsInfo, $this->tableIdsInfo[0], $this->tableIdsInfo[0]->name))
                        ? $this->tableIdsInfo[0]->name : null;
                $out = (is_array($id)) ? $id : [$idName => $id];

                // add via record, so batch processing can retrieve extras
                if ($requireMore) {
                    parent::addToTransaction($id);
                }
                break;

            case Verbs::PUT:
            case Verbs::MERGE:
            case Verbs::PATCH:
                if (!empty($updates)) {
                    $record = $updates;
                }

//                if (!empty($relatedInfo)) {
//                    $this->updatePreRelations($record, $relatedInfo);
//                }
                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters, true);

                // only update by ids can use batching, too complicated with ssFilters and related update
//                if ( !$needToIterate && !empty( $updates ) )
//                {
//                    return parent::addToTransaction( null, $id );
//                }

                if (!empty($parsed)) {
                    $rows = $builder->update($parsed);
                    if (0 >= $rows) {
                        // could have just not updated anything, or could be bad id
                        $result = $this->runQuery(
                            $this->transactionTable,
                            $fields,
                            $builder,
                            $extras
                        );
                        if (empty($result)) {
                            throw new NotFoundException("Record with identifier '" .
                                print_r($id, true) .
                                "' not found.");
                        }
                    }
                }

                if (!empty($relatedInfo)) {
                    // need the id back in the record
                    if (!empty($id)) {
                        if (is_array($id)) {
                            $record = array_merge($record, $id);
                        } else {
                            $record[array_get($idFields, 0)] = $id;
                        }
                    }
//                    $this->updatePostRelations(
//                        $this->transactionTable,
//                        $record,
//                        $relatedInfo,
//                        $allowRelatedDelete
//                    );
                }

                $idName =
                    (isset($this->tableIdsInfo, $this->tableIdsInfo[0], $this->tableIdsInfo[0]->name))
                        ? $this->tableIdsInfo[0]->name : null;
                $out = (is_array($id)) ? $id : [$idName => $id];

                // add via record, so batch processing can retrieve extras
                if ($requireMore) {
                    parent::addToTransaction($id);
                }
                break;

            case Verbs::DELETE:
                if (!$needToIterate) {
                    return parent::addToTransaction(null, $id);
                }

                // add via record, so batch processing can retrieve extras
                if ($requireMore) {
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $fields,
                        $builder,
                        $extras
                    );
                    if (empty($result)) {
                        throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
                    }

                    $out = $result[0];
                }

                $rows = $builder->delete();
                if (0 >= $rows) {
                    if (empty($out)) {
                        // could have just not updated anything, or could be bad id
                        $result = $this->runQuery(
                            $this->transactionTable,
                            $fields,
                            $builder,
                            $extras
                        );
                        if (empty($result)) {
                            throw new NotFoundException("Record with identifier '" .
                                print_r($id, true) .
                                "' not found.");
                        }
                    }
                }

                if (empty($out)) {
                    $idName =
                        (isset($this->tableIdsInfo, $this->tableIdsInfo[0], $this->tableIdsInfo[0]->name))
                            ? $this->tableIdsInfo[0]->name : null;
                    $out = (is_array($id)) ? $id : [$idName => $id];
                }
                break;

            case Verbs::GET:
                if (!$needToIterate) {
                    return parent::addToTransaction(null, $id);
                }

                $result = $this->runQuery($this->transactionTable, $fields, $builder, $extras);
                if (empty($result)) {
                    throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
                }

                $out = $result[0];
                break;
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function commitTransaction($extras = null)
    {
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            return null;
        }

        $updates = array_get($extras, 'updates');
        $ssFilters = array_get($extras, 'ss_filters');
        $fields = array_get($extras, ApiOptions::FIELDS);
        $idFields = array_get($extras, 'id_fields');
        $related = array_get($extras, 'related');
        $requireMore = Scalar::boolval(array_get($extras, 'require_more')) || !empty($related);
        //$allowRelatedDelete = Scalar::boolval(array_get($extras, 'allow_related_delete'));
        //$relatedInfo = $this->describeTableRelated($this->transactionTable);

        $builder = $this->connection->table($this->transactionTable);

        /** @type ColumnSchema $idName */
        $idName = (isset($this->tableIdsInfo, $this->tableIdsInfo[0])) ? $this->tableIdsInfo[0] : null;
        if (empty($idName)) {
            throw new BadRequestException('No valid identifier found for this table.');
        }

        if (!empty($this->batchRecords)) {
            if (is_array($this->batchRecords[0])) {
                $temp = [];
                foreach ($this->batchRecords as $record) {
                    $temp[] = array_get($record, $idName->getName(true));
                }

                $builder->whereIn($idName->name, $temp);
            } else {
                $builder->whereIn($idName->name, $this->batchRecords);
            }
        } else {
            $builder->whereIn($idName->name, $this->batchIds);
        }

        $serverFilter = $this->buildQueryStringFromData($ssFilters);
        if (!empty($serverFilter)) {
            Session::replaceLookups($serverFilter);
            $params = [];
            $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
            $builder->whereRaw($filterString, $params);
        }

        $out = [];
        $action = $this->getAction();
        if (!empty($this->batchRecords)) {
            if (1 == count($this->tableIdsInfo)) {
                // records are used to retrieve extras
                // ids array are now more like records
                $fields = (empty($fields)) ? $idFields : $fields;
                $result = $this->runQuery($this->transactionTable, $fields, $builder, $extras);
                if (empty($result)) {
                    //throw new NotFoundException('No records were found using the given identifiers.');
                }

                $out = $result;
            } else {
                $out = $this->retrieveRecords($this->transactionTable, $this->batchRecords, $extras);
            }

            $this->batchRecords = [];
        } elseif (!empty($this->batchIds)) {
            switch ($action) {
                case Verbs::PUT:
                case Verbs::MERGE:
                case Verbs::PATCH:
                    if (!empty($updates)) {
                        $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                        if (!empty($parsed)) {
                            $rows = $builder->update($parsed);
                            if (0 >= $rows) {
                                throw new NotFoundException('No records were found using the given identifiers.');
                            }

                            if (count($this->batchIds) !== $rows) {
                                throw new BadRequestException('Batch Error: Not all requested records could be updated.');
                            }
                        }

//                        if ($requireMore) {
//                            $fields = (empty($fields)) ? $idFields : $fields;
//                            $result = $this->runQuery(
//                                $this->transactionTable,
//                                $fields,
//                                $builder,
//                                $extras
//                            );
//                            if (empty($result)) {
//                                throw new NotFoundException('No records were found using the given identifiers.');
//                            }
//
//                            $out = $result;
//                        }
                    }
                    break;

                case Verbs::DELETE:
                    if ($requireMore) {
                        $fields = (empty($fields)) ? $idFields : $fields;
                        $result = $this->runQuery(
                            $this->transactionTable,
                            $fields,
                            $builder,
                            $extras
                        );
                        if (count($this->batchIds) !== count($result)) {
                            $errors = [];
                            foreach ($this->batchIds as $index => $id) {
                                $found = false;
                                if (empty($result)) {
                                    foreach ($result as $record) {
                                        if ($id == array_get($record, $idName->getName(true))) {
                                            $out[$index] = $record;
                                            $found = true;
                                            continue;
                                        }
                                    }
                                }
                                if (!$found) {
                                    $errors[] = $index;
                                    $out[$index] = "Record with identifier '" . print_r($id, true) . "' not found.";
                                }
                            }
                        } else {
                            $out = $result;
                        }
                    }

                    $rows = $builder->delete();
                    if (count($this->batchIds) !== $rows) {
                        throw new BadRequestException('Batch Error: Not all requested records were deleted.');
                    }
                    break;

                case Verbs::GET:
                    $fields = (empty($fields)) ? $idFields : $fields;
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $fields,
                        $builder,
                        $extras
                    );
                    if (empty($result)) {
                        throw new NotFoundException('No records were found using the given identifiers.');
                    }

                    if (count($this->batchIds) !== count($result)) {
                        $errors = [];
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == array_get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    continue;
                                }
                            }
                            if (!$found) {
                                $errors[] = $index;
                                $out[$index] = "Record with identifier '" . print_r($id, true) . "' not found.";
                            }
                        }

                        if (!empty($errors)) {
                            $context = ['error' => $errors, ResourcesWrapper::getWrapper() => $out];
                            throw new NotFoundException('Batch Error: Not all records could be retrieved.', null, null,
                                $context);
                        }
                    }

                    $out = $result;
                    break;

                default:
                    break;
            }

            if (empty($out)) {
                $out = [];
                foreach ($this->batchIds as $id) {
                    $out[] = [$idName->getName(true) => $id];
                }
            }

            $this->batchIds = [];
        }

        if (0 < $this->connection->transactionLevel()) {
            $this->connection->commit();
        }

        return $out;
    }

    /** {@inheritdoc} */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        $fields = array_get($extras, ApiOptions::FIELDS);
        $ssFilters = array_get($extras, 'ss_filters');

        try {
            $fieldsInfo = $this->getFieldsInfo($table);

            // build filter string if necessary, add server-side filters if necessary
            /** @var CassandraBuilder $builder */
            $builder = $this->connection->table($table);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            //return $builder->get($fields);
            return $this->runQuery($table, $fields, $builder, $extras);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to retrieve records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * Take in a ANSI SQL filter string (WHERE clause)
     * or our generic NoSQL filter array or partial record
     * and parse it to the service's native filter criteria.
     * The filter string can have substitution parameters such as
     * ':name', in which case an associative array is expected,
     * for value substitution.
     *
     * @param \Illuminate\Database\Query\Builder $builder
     * @param string | array                     $filter       SQL WHERE clause filter string
     * @param array                              $params       Array of substitution values
     * @param array                              $ss_filters   Server-side filters to apply
     * @param array                              $avail_fields All available fields for the table
     *
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function convertFilterToNative(
        Builder $builder,
        $filter,
        $params = [],
        $ss_filters = [],
        $avail_fields = []
    ){
        // interpret any parameter values as lookups
        $params = (is_array($params) ? static::interpretRecordValues($params) : []);
        $serverFilter = $this->buildQueryStringFromData($ss_filters);

        $outParams = [];
        if (empty($filter)) {
            $filter = $serverFilter;
        } elseif (is_string($filter)) {
            if (!empty($serverFilter)) {
                $filter = '(' . $filter . ') ' . DbLogicalOperators::AND_STR . ' (' . $serverFilter . ')';
            }
        } elseif (is_array($filter)) {
            // todo parse client filter?
            $filter = '';
            if (!empty($serverFilter)) {
                $filter = '(' . $filter . ') ' . DbLogicalOperators::AND_STR . ' (' . $serverFilter . ')';
            }
        }

        Session::replaceLookups($filter);
        $filterString = $this->parseFilterString($filter, $outParams, $avail_fields, $params);
        if (!empty($filterString)) {
            $builder->whereRaw($filterString, $outParams);
        }
    }

    /**
     * @param array $record
     *
     * @return array
     */
    public static function interpretRecordValues($record)
    {
        if (!is_array($record) || empty($record)) {
            return $record;
        }

        foreach ($record as $field => $value) {
            Session::replaceLookups($value);
            static::valueToExpression($value);
            $record[$field] = $value;
        }

        return $record;
    }

    /**
     * @param $value
     */
    public static function valueToExpression(&$value)
    {
        if (is_array($value) && isset($value['expression'])) {
            $expression = $value['expression'];
            $params = [];
            if (is_array($expression) && isset($expression['value'])) {
                $params = isset($expression['params']) ? $expression['params'] : [];
                $expression = $expression['value'];
            }

            $value = new Expression($expression, $params);
        }
    }

    /**
     * @param       $filter_info
     *
     * @return null|string
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function buildQueryStringFromData($filter_info)
    {
        $filters = array_get($filter_info, 'filters');
        if (empty($filters)) {
            return null;
        }

        $sql = '';
        $combiner = array_get($filter_info, 'filter_op', DbLogicalOperators::AND_STR);
        foreach ($filters as $key => $filter) {
            if (!empty($sql)) {
                $sql .= " $combiner ";
            }

            $name = array_get($filter, 'name');
            $op = strtoupper(array_get($filter, 'operator'));
            if (empty($name) || empty($op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            if (DbComparisonOperators::requiresNoValue($op)) {
                $sql .= "($name $op)";
            } else {
                $value = array_get($filter, 'value');
                $sql .= "($name $op $value)";
            }
        }

        return $sql;
    }

    /**
     * @param string         $filter
     * @param array          $out_params
     * @param ColumnSchema[] $fields_info
     * @param array          $in_params
     *
     * @return string
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseFilterString($filter, array &$out_params, $fields_info, array $in_params = [])
    {
        if (empty($filter)) {
            return null;
        }

        $filter = trim($filter);
        // todo use smarter regex
        // handle logical operators first
        $logicalOperators = DbLogicalOperators::getDefinedConstants();
        foreach ($logicalOperators as $logicalOp) {
            if (DbLogicalOperators::NOT_STR === $logicalOp) {
                // NOT(a = 1)  or NOT (a = 1)format
                if ((0 === stripos($filter, $logicalOp . ' (')) || (0 === stripos($filter, $logicalOp . '('))) {
                    $parts = trim(substr($filter, 3));
                    $parts = $this->parseFilterString($parts, $out_params, $fields_info, $in_params);

                    return static::localizeOperator($logicalOp) . $parts;
                }
            } else {
                // (a = 1) AND (b = 2) format or (a = 1)AND(b = 2) format
                $filter = str_ireplace(')' . $logicalOp . '(', ') ' . $logicalOp . ' (', $filter);
                $paddedOp = ') ' . $logicalOp . ' (';
                if (false !== $pos = stripos($filter, $paddedOp)) {
                    $left = trim(substr($filter, 0, $pos)) . ')'; // add back right )
                    $right = '(' . trim(substr($filter, $pos + strlen($paddedOp))); // adding back left (
                    $left = $this->parseFilterString($left, $out_params, $fields_info, $in_params);
                    $right = $this->parseFilterString($right, $out_params, $fields_info, $in_params);

                    return $left . ' ' . static::localizeOperator($logicalOp) . ' ' . $right;
                }
            }
        }

        $wrap = false;
        if ((0 === strpos($filter, '(')) && ((strlen($filter) - 1) === strrpos($filter, ')'))) {
            // remove unnecessary wrapping ()
            $filter = substr($filter, 1, -1);
            $wrap = true;
        }

        // Some scenarios leave extra parens dangling
        $pure = trim($filter, '()');
        $pieces = explode($pure, $filter);
        $leftParen = (!empty($pieces[0]) ? $pieces[0] : null);
        $rightParen = (!empty($pieces[1]) ? $pieces[1] : null);
        $filter = $pure;

        // the rest should be comparison operators
        // Note: order matters here!
        $sqlOperators = DbComparisonOperators::getParsingOrder();
        foreach ($sqlOperators as $sqlOp) {
            $paddedOp = static::padOperator($sqlOp);
            if (false !== $pos = stripos($filter, $paddedOp)) {
                $field = trim(substr($filter, 0, $pos));
                $negate = false;
                if (false !== strpos($field, ' ')) {
                    $parts = explode(' ', $field);
                    $partsCount = count($parts);
                    if (($partsCount > 1) &&
                        (0 === strcasecmp($parts[$partsCount - 1], trim(DbLogicalOperators::NOT_STR)))
                    ) {
                        // negation on left side of operator
                        array_pop($parts);
                        $field = implode(' ', $parts);
                        $negate = true;
                    }
                }
                /** @type ColumnSchema $info */
                if (null === $info = array_get($fields_info, strtolower($field))) {
                    // This could be SQL injection attempt or bad field
                    throw new BadRequestException("Invalid or unparsable field in filter request: '$field'");
                }

                // make sure we haven't chopped off right side too much
                $value = trim(substr($filter, $pos + strlen($paddedOp)));
                if ((0 !== strpos($value, "'")) &&
                    (0 !== $lpc = substr_count($value, '(')) &&
                    ($lpc !== $rpc = substr_count($value, ')'))
                ) {
                    // add back to value from right
                    $parenPad = str_repeat(')', $lpc - $rpc);
                    $value .= $parenPad;
                    $rightParen = preg_replace('/\)/', '', $rightParen, $lpc - $rpc);
                }
                if (DbComparisonOperators::requiresValueList($sqlOp)) {
                    if ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                        // remove wrapping ()
                        $value = substr($value, 1, -1);
                        $parsed = [];
                        foreach (explode(',', $value) as $each) {
                            $parsed[] = $this->parseFilterValue(trim($each), $info, $out_params, $in_params);
                        }
                        $value = '(' . implode(',', $parsed) . ')';
                    } else {
                        throw new BadRequestException('Filter value lists must be wrapped in parentheses.');
                    }
                } elseif (DbComparisonOperators::requiresNoValue($sqlOp)) {
                    $value = null;
                } else {
                    static::modifyValueByOperator($sqlOp, $value);
                    $value = $this->parseFilterValue($value, $info, $out_params, $in_params);
                }

                $sqlOp = static::localizeOperator($sqlOp);
                if ($negate) {
                    $sqlOp = DbLogicalOperators::NOT_STR . ' ' . $sqlOp;
                }

                $out = $this->schema->parseFieldForFilter($info, true) . " $sqlOp";
                $out .= (isset($value) ? " $value" : null);
                if ($leftParen) {
                    $out = $leftParen . $out;
                }
                if ($rightParen) {
                    $out .= $rightParen;
                }

                return ($wrap ? '(' . $out . ')' : $out);
            }
        }

        // This could be SQL injection attempt or unsupported filter arrangement
        throw new BadRequestException('Invalid or unparsable filter request.');
    }

    /**
     * @param mixed        $value
     * @param ColumnSchema $info
     * @param array        $out_params
     * @param array        $in_params
     *
     * @return int|null|string
     * @throws BadRequestException
     */
    protected function parseFilterValue($value, ColumnSchema $info, array &$out_params, array $in_params = [])
    {
        // if a named replacement parameter, un-name it because Laravel can't handle named parameters
        if (is_array($in_params) && (0 === strpos($value, ':'))) {
            if (array_key_exists($value, $in_params)) {
                $value = $in_params[$value];
            }
        }

        // remove quoting on strings if used, i.e. 1.x required them
        if (is_string($value)) {

            if ((0 === strcmp("'" . trim($value, "'") . "'", $value)) ||
                (0 === strcmp('"' . trim($value, '"') . '"', $value))
            ) {
                $value = substr($value, 1, -1);
            } elseif ((0 === strpos($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                // function call
                return $value;
            }
        }
        // if not already a replacement parameter, evaluate it
        try {
            $value = $this->schema->parseValueForSet($value, $info);
        } catch (ForbiddenException $ex) {
            // need to prop this up?
        }

        switch ($cnvType = $this->schema->determinePhpConversionType($info->type)) {
            case 'int':
                if (!is_int($value)) {
                    if (!(ctype_digit($value))) {
                        throw new BadRequestException("Field '{$info->getName(true)}' must be a valid integer.");
                    } else {
                        $value = intval($value);
                    }
                }
                break;

            case 'time':
                $cfgFormat = Config::get('df.db_time_format');
                $outFormat = 'H:i:s.u';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'date':
                $cfgFormat = Config::get('df.db_date_format');
                $outFormat = 'Y-m-d';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'datetime':
                $cfgFormat = Config::get('df.db_datetime_format');
                $outFormat = 'Y-m-d H:i:s';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'timestamp':
                $cfgFormat = Config::get('df.db_timestamp_format');
                $outFormat = 'Y-m-d H:i:s';
                $value = DataFormatter::formatDateTime($outFormat, $value, $cfgFormat);
                break;
            case 'bool':
                $value = boolval(('false' === trim(strtolower($value)))? 0 : $value);
                break;

            default:
                break;
        }

        $out_params[] = $value;
        $value = '?';

        return $value;
    }

    /**
     * @param                                    $table
     * @param                                    $select
     * @param \Illuminate\Database\Query\Builder $builder
     * @param                                    $extras
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     * @throws \DreamFactory\Core\Exceptions\NotFoundException
     * @throws \DreamFactory\Core\Exceptions\RestException
     */
    protected function runQuery($table, $select, Builder $builder, $extras)
    {
        $schema = $this->schema->getResource(DbResourceTypes::TYPE_TABLE, $table);
        if (!$schema) {
            throw new NotFoundException("Table '$table' does not exist in the database.");
        }

        $order = trim(array_get($extras, ApiOptions::ORDER));
        $group = trim(array_get($extras, ApiOptions::GROUP));
        $limit = intval(array_get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(array_get($extras, ApiOptions::OFFSET, 0));
        $includeCount = Scalar::boolval(array_get($extras, ApiOptions::INCLUDE_COUNT));
        //$related = array_get($extras, ApiOptions::RELATED);
        /** @type ColumnSchema[] $availableFields */
        $availableFields = $schema->getColumns(true);
        ///** @type RelationSchema[] $availableRelations */
        //$availableRelations = $schema->getRelations(true);
        $maxAllowed = static::getMaxRecordsReturnedLimit();
        $needLimit = false;

        $result = $this->parseSelect($select, $availableFields);
        $bindings = array_get($result, 'bindings');
        $select = array_get($result, 'fields');
        $select = (empty($select)) ? '*' : $select;
        // see if we need to add anymore fields to select for related retrieval
        /*if (('*' !== $select) && !empty($availableRelations) && (!empty($related) || $schema->fetchRequiresRelations)) {
            foreach ($availableRelations as $relation) {
                if (false === array_search($relation->field, $select)) {
                    $select[] = $relation->field;
                }
            }
        }*/

        if (($limit < 1) || ($limit > $maxAllowed)) {
            // impose a limit to protect server
            $limit = $maxAllowed;
            $needLimit = true;
        }

        // count total records
        $count = ($includeCount || $needLimit) ? $builder->count() : 0;

        // apply the rest of the parameters
        $builder->select($select);

        if (!empty($order)) {
            if (false !== strpos($order, ';')) {
                throw new BadRequestException('Invalid order by clause in request.');
            }
            $commas = explode(',', $order);
            switch (count($commas)) {
                case 0:
                    break;
                case 1:
                    $spaces = explode(' ', $commas[0]);
                    $orderField = $spaces[0];
                    $direction = (isset($spaces[1]) ? $spaces[1] : 'asc');
                    $builder->orderBy($orderField, $direction);
                    break;
                default:
                    // todo need to validate format here first
                    $builder->orderByRaw($order);
                    break;
            }
        }
        if (!empty($group)) {
            if (false !== strpos($order, ';')) {
                throw new BadRequestException('Invalid group by clause in request.');
            }
            $groups = $this->parseGroupBy($group, $availableFields);
            $builder->groupBy($groups);
        }
        $builder->take($limit);
        $builder->skip($offset);

        $result = $builder->get();
        $data = [];
        $row = 0;
        foreach ($result as $record) {
            $temp = (array)$record;
            foreach ($bindings as $binding) {
                $name = array_get($binding, 'name');
                $type = array_get($binding, 'php_type');
                if (isset($temp[$name])) {
                    $temp[$name] = $this->schema->formatValue($temp[$name], $type);
                }
            }

            $data[$row++] = $temp;
        }

        $meta = [];
        if ($includeCount || $needLimit) {
            if ($includeCount || $count > $maxAllowed) {
                $meta['count'] = $count;
            }
            if (($count - $offset) > $limit) {
                $meta['next'] = $offset + $limit;
            }
        }

        if (Scalar::boolval(array_get($extras, ApiOptions::INCLUDE_SCHEMA))) {
            try {
                $meta['schema'] = $schema->toArray(true);
            } catch (RestException $ex) {
                throw $ex;
            } catch (\Exception $ex) {
                throw new InternalServerErrorException("Error describing database table '$table'.\n" .
                    $ex->getMessage(), $ex->getCode());
            }
        }

//        if (!empty($related) || $schema->fetchRequiresRelations) {
//            if (!empty($availableRelations)) {
//                foreach ($data as $key => $temp) {
//                    $data[$key] = $this->retrieveRelatedRecords($schema, $availableRelations, $related, $temp);
//                }
//            }
//        }

        if (!empty($meta)) {
            $data['meta'] = $meta;
        }

        return $data;
    }

    /**
     * @param  string|array   $fields
     * @param  ColumnSchema[] $avail_fields
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseSelect($fields, $avail_fields)
    {
        $outArray = [];
        $bindArray = [];
        if (!(empty($fields) || (ApiOptions::FIELDS_ALL === $fields))) {
            $fields = (!is_array($fields)) ? array_map('trim', explode(',', trim($fields, ','))) : $fields;
            foreach ($fields as $field) {
                $ndx = strtolower($field);
                if (!isset($avail_fields[$ndx])) {
                    throw new BadRequestException('Invalid field requested: ' . $field);
                }

                $fieldInfo = $avail_fields[$ndx];
                $bindArray[] = $this->schema->getPdoBinding($fieldInfo);
                $outArray[] = $this->schema->parseFieldForSelect($fieldInfo, false);
            }
        } else {
            foreach ($avail_fields as $fieldInfo) {
                if ($fieldInfo->isAggregate()) {
                    continue;
                }
                $bindArray[] = $this->schema->getPdoBinding($fieldInfo);
                $outArray[] = $this->schema->parseFieldForSelect($fieldInfo, false);
            }
        }

        return ['fields' => $outArray, 'bindings' => $bindArray];
    }

    /**
     * @param  string|array   $fields
     * @param  ColumnSchema[] $avail_fields
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseGroupBy($fields, $avail_fields)
    {
        $outArray = [];
        if (!empty($fields)) {
            $fields = (!is_array($fields)) ? array_map('trim', explode(',', trim($fields, ','))) : $fields;
            foreach ($fields as $field) {
                $ndx = strtolower($field);
                if (!isset($avail_fields[$ndx])) {
                    $outArray[] = DB::raw($field);
                } else {
                    $fieldInfo = $avail_fields[$ndx];
                    $outArray[] = $fieldInfo->name;
                }
            }
        }

        return $outArray;
    }

    /**
     * @param array          $record
     * @param ColumnSchema[] $fields_info
     * @param array          $filter_info
     * @param bool           $for_update
     * @param array          $old_record
     *
     * @return array
     * @throws \Exception
     */
    protected function parseRecord($record, $fields_info, $filter_info = null, $for_update = false, $old_record = null)
    {
        $record = $this->interpretRecordValues($record);

        $parsed = (empty($fields_info)) ? $record : [];
        if (!empty($fields_info)) {
            $record = array_change_key_case($record, CASE_LOWER);

            foreach ($fields_info as $fieldInfo) {
                // add or override for specific fields
                switch ($fieldInfo->type) {
                    case DbSimpleTypes::TYPE_UUID:
                        $fieldVal = $record[$fieldInfo->getName(true)];
                        $parsed[$fieldInfo->name] = new \Cassandra\Uuid($fieldVal);
                        break;
                    case DbSimpleTypes::TYPE_TIME_UUID:
                        $fieldVal = $record[$fieldInfo->getName(true)];
                        $parsed[$fieldInfo->name] = new \Cassandra\Timeuuid($fieldVal);
                        break;
                    case DbSimpleTypes::TYPE_TIMESTAMP_ON_CREATE:
                        if (!$for_update) {
                            $parsed[$fieldInfo->name] = $this->getCurrentTimestamp();
                        }
                        break;
                    case DbSimpleTypes::TYPE_TIMESTAMP_ON_UPDATE:
                        $parsed[$fieldInfo->name] = $this->getCurrentTimestamp();
                        break;
                    case DbSimpleTypes::TYPE_USER_ID_ON_CREATE:
                        if (!$for_update) {
                            $userId = Session::getCurrentUserId();
                            if (isset($userId)) {
                                $parsed[$fieldInfo->name] = $userId;
                            }
                        }
                        break;
                    case DbSimpleTypes::TYPE_USER_ID_ON_UPDATE:
                        $userId = Session::getCurrentUserId();
                        if (isset($userId)) {
                            $parsed[$fieldInfo->name] = $userId;
                        }
                        break;
                    default:
                        $name = strtolower($fieldInfo->getName(true));
                        // need to check for virtual or api_read_only validation here.
                        if ((DbSimpleTypes::TYPE_VIRTUAL === $fieldInfo->type) ||
                            isset($fieldInfo->validation, $fieldInfo->validation['api_read_only'])
                        ) {
                            unset($record[$name]);
                            continue;
                        }
                        // need to check for id in record and remove it, as some DBs complain.
                        if ($for_update && (DbSimpleTypes::TYPE_ID === $fieldInfo->type)) {
                            unset($record[$name]);
                            continue;
                        }
                        if (array_key_exists($name, $record)) {
                            $fieldVal = array_get($record, $name);
                            // due to conversion from XML to array, null or empty xml elements have the array value of an empty array
                            if (is_array($fieldVal) && empty($fieldVal)) {
                                $fieldVal = null;
                            }

                            if (is_null($fieldVal) && !$fieldInfo->allowNull) {
                                throw new BadRequestException("Field '$name' can not be NULL.");
                            }

                            /** validations **/
                            if (!static::validateFieldValue(
                                $fieldInfo->getName(true),
                                $fieldVal,
                                $fieldInfo->validation,
                                $for_update,
                                $fieldInfo
                            )
                            ) {
                                // if invalid and exception not thrown, drop it
                                unset($record[$name]);
                                continue;
                            }

                            try {
                                $fieldVal = $this->parseValueForSet($fieldVal, $fieldInfo);
                            } catch (ForbiddenException $ex) {
                                unset($record[$name]);
                                continue;
                            }

                            $parsed[$fieldInfo->name] = $fieldVal;
                            unset($record[$name]);
                        } else {
                            // if field is required, kick back error
                            if ($fieldInfo->getRequired() && !$for_update) {
                                throw new BadRequestException("Required field '$name' can not be NULL.");
                            }
                            break;
                        }
                        break;
                }
            }
        }

        if (!empty($filter_info)) {
            $this->validateRecord($parsed, $filter_info, $for_update, $old_record);
        }

        return $parsed;
    }

    /**
     * @param string $table
     * @param array  $records
     * @param array  $extras
     *
     * @throws \Exception
     * @return array
     */
    public function patchRecords($table, $records, $extras = [])
    {
        $records = static::validateAsArray($records, null, true, 'The request contains no valid record sets.');

        $fields = array_get($extras, ApiOptions::FIELDS);
        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);
        $isSingle = (1 == count($records));
        $rollback = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::ROLLBACK, false));
        $continue = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::CONTINUES, false));
        if ($rollback && $continue) {
            throw new BadRequestException('Rollback and continue operations can not be requested at the same time.');
        }

        $this->initTransaction($table, $idFields, $idTypes);

        $extras['id_fields'] = $idFields;
        $extras['require_more'] = static::requireMoreFields($fields, $idFields);

        $out = [];
        $errors = [];
        try {
            foreach ($records as $index => $record) {
                try {
                    if (false === $id = static::checkForIds($record, $this->tableIdsInfo, $extras)) {
                        throw new BadRequestException("Required id field(s) not found in record $index: " .
                            print_r($record, true));
                    }

                    $idFieldInfo = $this->tableIdsInfo[0];
                    unset($record[$idFieldInfo->getName()]);

                    $result = $this->addToTransaction($record, $id, $extras, $rollback, $continue, $isSingle);
                    if (isset($result)) {
                        // operation performed, take output
                        $out[$index] = $result;
                    }
                } catch (\Exception $ex) {
                    if ($isSingle || $rollback || !$continue) {
                        if (0 !== $index) {
                            // first error, don't worry about batch just throw it
                            // mark last error and index for batch results
                            $errors[] = $index;
                            $out[$index] = $ex->getMessage();
                        }

                        throw $ex;
                    }

                    // mark error and index for batch results
                    $errors[] = $index;
                    $out[$index] = $ex->getMessage();
                }
            }

            if (!empty($errors)) {
                throw new BadRequestException();
            }

            $result = $this->commitTransaction($extras);
            if (isset($result)) {
                $out = $result;
            }

            return $out;
        } catch (\Exception $ex) {
            $msg = $ex->getMessage();

            $context = null;
            if (!empty($errors)) {
                $wrapper = ResourcesWrapper::getWrapper();
                $context = ['error' => $errors, $wrapper => $out];
                $msg = 'Batch Error: Not all records could be patched.';
            }

            if ($rollback) {
                $this->rollbackTransaction();

                $msg .= " All changes rolled back.";
            }

            if ($ex instanceof RestException) {
                $context = (empty($temp)) ? $context : $temp;
                $ex->setContext($context);
                $ex->setMessage($msg);
                throw $ex;
            }

            throw new InternalServerErrorException("Failed to patch records in '$table'.\n$msg", null, null,
                $context);
        }
    }

    /**
     * @param string $table
     * @param array  $records
     * @param array  $extras
     *
     * @throws \Exception
     * @return array
     */
    public function updateRecords($table, $records, $extras = [])
    {
        $records = static::validateAsArray($records, null, true, 'The request contains no valid record sets.');

        $fields = array_get($extras, ApiOptions::FIELDS);
        $idFields = array_get($extras, ApiOptions::ID_FIELD);
        $idTypes = array_get($extras, ApiOptions::ID_TYPE);
        $isSingle = (1 == count($records));
        $rollback = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::ROLLBACK, false));
        $continue = ($isSingle) ? false : Scalar::boolval(array_get($extras, ApiOptions::CONTINUES, false));
        if ($rollback && $continue) {
            throw new BadRequestException('Rollback and continue operations can not be requested at the same time.');
        }

        $this->initTransaction($table, $idFields, $idTypes);

        $extras['id_fields'] = $idFields;
        $extras['require_more'] = static::requireMoreFields($fields, $idFields);

        $out = [];
        $errors = [];
        try {
            foreach ($records as $index => $record) {
                try {
                    if (false === $id = static::checkForIds($record, $this->tableIdsInfo, $extras)) {
                        throw new BadRequestException("Required id field(s) not found in record $index: " .
                            print_r($record, true));
                    }

                    $idFieldInfo = $this->tableIdsInfo[0];
                    unset($record[$idFieldInfo->getName()]);

                    $result = $this->addToTransaction($record, $id, $extras, $rollback, $continue, $isSingle);
                    if (isset($result)) {
                        // operation performed, take output
                        $out[$index] = $result;
                    }
                } catch (\Exception $ex) {
                    if ($isSingle || $rollback || !$continue) {
                        if (0 !== $index) {
                            // first error, don't worry about batch just throw it
                            // mark last error and index for batch results
                            $errors[] = $index;
                            $out[$index] = $ex->getMessage();
                        }

                        throw $ex;
                    }

                    // mark error and index for batch results
                    $errors[] = $index;
                    $out[$index] = $ex->getMessage();
                }
            }

            if (!empty($errors)) {
                throw new BadRequestException();
            }

            $result = $this->commitTransaction($extras);
            if (isset($result)) {
                $out = $result;
            }

            return $out;
        } catch (\Exception $ex) {
            $msg = $ex->getMessage();

            $context = null;
            if (!empty($errors)) {
                $wrapper = ResourcesWrapper::getWrapper();
                $context = ['error' => $errors, $wrapper => $out];
                $msg = 'Batch Error: Not all records could be updated.';
            }

            if ($rollback) {
                $this->rollbackTransaction();

                $msg .= " All changes rolled back.";
            }

            if ($ex instanceof RestException) {
                $context = (empty($temp)) ? $context : $temp;
                $ex->setContext($context);
                $ex->setMessage($msg);
                throw $ex;
            }

            throw new InternalServerErrorException("Failed to update records in '$table'.\n$msg", null, null,
                $context);
        }
    }
}