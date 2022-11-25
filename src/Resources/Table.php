<?php
namespace DreamFactory\Core\Cassandra\Resources;

use DB;
use DreamFactory\Core\Database\Resources\BaseDbTableResource;
use DreamFactory\Core\Database\Schema\ColumnSchema;
use DreamFactory\Core\Database\Components\Expression;
use DreamFactory\Core\Database\Enums\DbFunctionUses;
use DreamFactory\Core\Database\Schema\TableSchema;
use DreamFactory\Core\Enums\ApiOptions;
use DreamFactory\Core\Enums\DbLogicalOperators;
use DreamFactory\Core\Enums\DbComparisonOperators;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\Exceptions\BatchException;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Exceptions\NotFoundException;
use DreamFactory\Core\Exceptions\RestException;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Core\Enums\Verbs;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Database\Query\Builder;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;

class Table extends BaseDbTableResource
{
    /**
     * {@inheritdoc}
     */
    public function updateRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        $record = static::validateAsArray($record, null, false, 'There are no fields in the record.');

        $idFields = Arr::get($extras, ApiOptions::ID_FIELD);
        $idTypes = Arr::get($extras, ApiOptions::ID_TYPE);
        $related = Arr::get($extras, ApiOptions::RELATED);
        $allowRelatedDelete = array_get_bool($extras, ApiOptions::ALLOW_RELATED_DELETE);
        $ssFilters = Arr::get($extras, 'ss_filters');

        try {
            if (!$tableSchema = $this->parent->getTableSchema($table)) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }

            $fieldsInfo = $tableSchema->getColumns(true);
            $idsInfo = $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);
            $relatedInfo = $tableSchema->getRelations(true);
            $parsed = $this->parseRecord($record, $fieldsInfo, $ssFilters, true);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            if (!empty($parsed)) {
                $builder->update($parsed);
            }

            $results = $this->runQuery($table, $builder, $extras);

            if (!empty($relatedInfo)) {
                // update related info
                foreach ($results as $row) {
                    $this->checkForIds($row, $idsInfo, $extras);
                    $this->updatePostRelations($table, array_merge($row, $record), $relatedInfo, $allowRelatedDelete);
                }
                // get latest with related changes if requested
                if (!empty($related)) {
                    $results = $this->runQuery($table, $builder, $extras);
                }
            }

            return $results;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to update records in '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function patchRecordsByFilter($table, $record, $filter = null, $params = [], $extras = [])
    {
        // currently the same as update here
        return $this->updateRecordsByFilter($table, $record, $filter, $params, $extras);
    }

    /**
     * {@inheritdoc}
     */
    public function truncateTable($table, $extras = [])
    {
        // truncate the table, return success
        try {
            if (!$tableSchema = $this->parent->getTableSchema($table)) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }
            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
            $ssFilters = \Illuminate\Support\Arr::get($extras, 'ss_filters');
            $params = [];
            $serverFilter = $this->buildQueryStringFromData($ssFilters);
            if (!empty($serverFilter)) {
                Session::replaceLookups($serverFilter);
                $filterString = $this->parseFilterString($serverFilter, $params, $this->tableFieldsInfo);
                $builder->whereRaw($filterString, $params);
                $builder->delete();
            } else {
                $builder->truncate();
            }

            return ['success' => true];
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function deleteRecordsByFilter($table, $filter, $params = [], $extras = [])
    {
        if (empty($filter)) {
            throw new BadRequestException("Filter for delete request can not be empty.");
        }

        $idFields = Arr::get($extras, ApiOptions::ID_FIELD);
        $idTypes = Arr::get($extras, ApiOptions::ID_TYPE);
        $ssFilters = Arr::get($extras, 'ss_filters');

        try {
            if (!$tableSchema = $this->parent->getTableSchema($table)) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }
            $fieldsInfo = $tableSchema->getColumns(true);
            /*$idsInfo = */
            $this->getIdsInfo($table, $fieldsInfo, $idFields, $idTypes);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            $results = $this->runQuery($table, $builder, $extras);

            $builder->delete();

            return $results;
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to delete records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * {@inheritdoc}
     */
    public function retrieveRecordsByFilter($table, $filter = null, $params = [], $extras = [])
    {
        $ssFilters = Arr::get($extras, 'ss_filters');

        try {
            $tableSchema = $this->parent->getTableSchema($table);
            if (!$tableSchema) {
                throw new NotFoundException("Table '$table' does not exist in the database.");
            }

            $fieldsInfo = $tableSchema->getColumns(true);

            // build filter string if necessary, add server-side filters if necessary
            $builder = $this->parent->getConnection()->table($tableSchema->internalName);
            $this->convertFilterToNative($builder, $filter, $params, $ssFilters, $fieldsInfo);

            return $this->runQuery($table, $builder, $extras);
        } catch (RestException $ex) {
            throw $ex;
        } catch (\Exception $ex) {
            throw new InternalServerErrorException("Failed to retrieve records from '$table'.\n{$ex->getMessage()}");
        }
    }

    /**
     * @param              $table
     * @param array        $extras
     * @throws BadRequestException
     * @throws InternalServerErrorException
     * @throws NotFoundException
     * @throws RestException
     */
    protected function runQuery($table, Builder $builder, $extras): int|array
    {
        $schema = $this->parent->getTableSchema($table);
        if (!$schema) {
            throw new NotFoundException("Table '$table' does not exist in the database.");
        }

        $limit = intval(Arr::get($extras, ApiOptions::LIMIT, 0));
        $offset = intval(Arr::get($extras, ApiOptions::OFFSET, 0));
        $countOnly = array_get_bool($extras, ApiOptions::COUNT_ONLY);
        $includeCount = array_get_bool($extras, ApiOptions::INCLUDE_COUNT);

        $maxAllowed = $this->getMaxRecordsReturnedLimit();
        $needLimit = false;
        if (($limit < 1) || ($limit > $maxAllowed)) {
            // impose a limit to protect server
            $limit = $maxAllowed;
            $needLimit = true;
        }

        // count total records
        $count = 0;
        if ($countOnly || $includeCount || $needLimit) {
            $count = $builder->count([DB::raw('1')]);
        }

        if ($countOnly) {
            return $count;
        }

        // apply the selected fields
        $select = $this->parseSelect($schema, $extras);
        $builder->select($select);

        // apply the rest of the parameters
        $order = trim((string) Arr::get($extras, ApiOptions::ORDER));
        if (!empty($order)) {
            if (Str::contains($order, ';')) {
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
        $group = trim((string) Arr::get($extras, ApiOptions::GROUP));
        if (!empty($group)) {
            $group = static::fieldsToArray($group);
            $groups = $this->parseGroupBy($schema, $group);
            $builder->groupBy($groups);
        }
        $builder->take($limit);
        $builder->skip($offset);

        $result = $this->getQueryResults($schema, $builder, $extras);

        $meta = [];
        if ($includeCount || $needLimit) {
            if ($includeCount || $count > $maxAllowed) {
                $meta['count'] = $count;
            }
            if (($count - $offset) > $limit) {
                $meta['next'] = $offset + $limit;
            }
        }

        if (array_get_bool($extras, ApiOptions::INCLUDE_SCHEMA)) {
            try {
                $meta['schema'] = $schema->toArray(true);
            } catch (RestException $ex) {
                throw $ex;
            } catch (\Exception $ex) {
                throw new InternalServerErrorException("Error describing database table '$table'.\n" .
                    $ex->getMessage(), $ex->getCode());
            }
        }

        $data = $result->toArray();
        if (!empty($meta)) {
            $data['meta'] = $meta;
        }

        return $data;
    }

    /**
     * @param array       $extras
     * @return Collection
     */
    protected function getQueryResults(TableSchema $schema, Builder $builder, $extras)
    {
        $result = $builder->get();

        $result->transform(function ($item) use ($schema) {
            $item = (array)$item;
            foreach ($item as $field => &$value) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $value = $this->parent->getSchema()->typecastToClient($value, $fieldInfo);
                }
            }

            return $item;
        });

        return $result;
    }

    /**
     * Take in a ANSI SQL filter string (WHERE clause)
     * or our generic NoSQL filter array or partial record
     * and parse it to the service's native filter criteria.
     * The filter string can have substitution parameters such as
     * ':name', in which case an associative array is expected,
     * for value substitution.
     *
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
        string|array $filter,
        $params = [],
        $ss_filters = [],
        $avail_fields = []
    ) {
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
     * @param       $filter_info
     *
     * @return null|string
     * @throws \DreamFactory\Core\Exceptions\InternalServerErrorException
     */
    protected function buildQueryStringFromData($filter_info)
    {
        $filters = Arr::get($filter_info, 'filters');
        if (empty($filters)) {
            return null;
        }

        $sql = '';
        $combiner = Arr::get($filter_info, 'filter_op', DbLogicalOperators::AND_STR);
        foreach ($filters as $key => $filter) {
            if (!empty($sql)) {
                $sql .= " $combiner ";
            }

            $name = Arr::get($filter, 'name');
            $op = strtoupper((string) Arr::get($filter, 'operator'));
            if (empty($name) || empty($op)) {
                // log and bail
                throw new InternalServerErrorException('Invalid server-side filter configuration detected.');
            }

            if (DbComparisonOperators::requiresNoValue($op)) {
                $sql .= "($name $op)";
            } else {
                $value = Arr::get($filter, 'value');
                $sql .= "($name $op $value)";
            }
        }

        return $sql;
    }

    /**
     * @param string         $filter
     * @param ColumnSchema[] $fields_info
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
                // NOT(a = 1) or NOT (a = 1)format
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
        if ((str_starts_with($filter, '(')) && ((strlen($filter) - 1) === strrpos($filter, ')'))) {
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
            if (false !== $pos = stripos($filter, (string) $paddedOp)) {
                $field = trim(substr($filter, 0, $pos));
                $negate = false;
                if (Str::contains($field, ' ')) {
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
                if (null === $info = Arr::get($fields_info, strtolower($field))) {
                    // This could be SQL injection attempt or bad field
                    throw new BadRequestException("Invalid or unparsable field in filter request: '$field'");
                }

                // make sure we haven't chopped off right side too much
                $value = trim(substr($filter, $pos + strlen((string) $paddedOp)));
                if ((!str_starts_with($value, "'")) &&
                    (0 !== $lpc = substr_count($value, '(')) &&
                    ($lpc !== $rpc = substr_count($value, ')'))
                ) {
                    // add back to value from right
                    $parenPad = str_repeat(')', $lpc - $rpc);
                    $value .= $parenPad;
                    $rightParen = preg_replace('/\)/', '', $rightParen, $lpc - $rpc);
                }
                if (DbComparisonOperators::requiresValueList($sqlOp)) {
                    if ((str_starts_with($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
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

                if ($function = $info->getDbFunction(DbFunctionUses::FILTER)) {
                    $out = $this->parent->getConnection()->raw($function);
                } else {
                    $out = $info->quotedName;
                }
                $out .= " $sqlOp";
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
     *
     * @return int|null|string
     * @throws BadRequestException
     */
    protected function parseFilterValue(mixed $value, ColumnSchema $info, array &$out_params, array $in_params = [])
    {
        // if a named replacement parameter, un-name it because Laravel can't handle named parameters
        if (is_array($in_params) && (str_starts_with((string) $value, ':'))) {
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
            } elseif ((str_starts_with($value, '(')) && ((strlen($value) - 1) === strrpos($value, ')'))) {
                // function call
                return $value;
            }
        }

        // anything else schema specific
        $value = $this->parent->getSchema()->typecastToNative($value, $info);

        $out_params[] = $value;
        $value = '?';

        return $value;
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
     * @param ColumnSchema $field
     */
    protected function parseFieldForSelect($field): \Illuminate\Database\Query\Expression|string
    {
        if ($function = $field->getDbFunction(DbFunctionUses::SELECT)) {
            return $this->parent->getConnection()->raw($function . ' AS ' . $field->getName(true, true));
        }

        $out = $field->name;
        if (!empty($field->alias)) {
            $out .= ' AS ' . $field->alias;
        }

        return $out;
    }

    protected static function fieldsToArray($fields)
    {
        if (empty($fields) || (ApiOptions::FIELDS_ALL === $fields)) {
            return [];
        }

        return (!is_array($fields)) ? array_map('trim', explode(',', trim((string) $fields, ','))) : $fields;
    }

    /**
     * @param  TableSchema $schema
     * @param  array|null  $extras
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseSelect($schema, $extras)
    {
        $fields = Arr::get($extras, ApiOptions::FIELDS);
        if (empty($fields)) {
            // minimally return id fields
            $idFields = Arr::get($extras, ApiOptions::ID_FIELD);
            if (empty($idFields)) {
                $idFields = $schema->primaryKey;
            }
            $fields = $idFields;
        }
        $fields = static::fieldsToArray($fields);
        $outArray = [];
        if (empty($fields)) {
            foreach ($schema->getColumns() as $fieldInfo) {
                if ($fieldInfo->isAggregate) {
                    continue;
                }
                $out = $this->parseFieldForSelect($fieldInfo);
                if (is_array($out)) {
                    $outArray = array_merge($outArray, $out);
                } else {
                    $outArray[] = $out;
                }
            }
        } else {
            $related = Arr::get($extras, ApiOptions::RELATED);
            $related = static::fieldsToArray($related);
            if (!empty($related) || $schema->fetchRequiresRelations) {
                // add any required relationship mapping fields
                foreach ($schema->getRelations() as $relation) {
                    if ($relation->alwaysFetch || in_array($relation->getName(true), $related)) {
                        foreach ($relation->field as $relField) {
                            if ($fieldInfo = $schema->getColumn($relField)) {
                                $relationField = $fieldInfo->getName(true); // account for aliasing
                                if (false === array_search($relationField, $fields)) {
                                    $fields[] = $relationField;
                                }
                            }
                        }
                    }
                }
            }
            foreach ($fields as $field) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $out = $this->parseFieldForSelect($fieldInfo);
                    if (is_array($out)) {
                        $outArray = array_merge($outArray, $out);
                    } else {
                        $outArray[] = $out;
                    }
                } else {
                    throw new BadRequestException('Invalid field requested: ' . $field);
                }
            }
        }

        return $outArray;
    }

    /**
     * @param  TableSchema $schema
     * @param  array       $fields
     *
     * @return array
     * @throws \DreamFactory\Core\Exceptions\BadRequestException
     * @throws \Exception
     */
    protected function parseGroupBy($schema, $fields = null)
    {
        $outArray = [];
        if (!empty($fields)) {
            foreach ($fields as $field) {
                if ($fieldInfo = $schema->getColumn($field, true)) {
                    $outArray[] = $fieldInfo->name;
                } else {
                    if (Str::contains((string) $field, ';')) {
                        throw new BadRequestException('Invalid group by clause in request.');
                    }
                    $outArray[] = DB::raw($field); // todo better checks on group by clause
                }
            }
        }

        return $outArray;
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
                    $ndx = strtolower((string) $field);
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
    ) {
        $dbConn = $this->parent->getConnection();
        if ($rollback) {
            // sql transaction really only for rollback scenario, not batching
            if (0 >= $dbConn->transactionLevel()) {
                $dbConn->beginTransaction();
            }
        }

        $ssFilters = Arr::get($extras, 'ss_filters');
        $updates = Arr::get($extras, 'updates');
        $idFields = Arr::get($extras, 'id_fields');
        $needToIterate = ($single || !$continue || (1 < count($this->tableIdsInfo)));

        $related = Arr::get($extras, 'related');
        $requireMore = array_get_bool($extras, 'require_more') || !empty($related);

        $builder = $dbConn->table($this->transactionTableSchema->internalName);
        $match = [];
        if (!is_null($id)) {
            if (is_array($id)) {
                $match = $id;
                foreach ($idFields as $name) {
                    $builder->where($name, Arr::get($id, $name));
                }
            } else {
                $name = Arr::get($idFields, 0);
                $match[$name] = $id;
                $builder->where($name, $id);
            }
        }

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
                if (!empty($match)) {
                    $record = array_merge($record, $match);
                }

                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters);
                if (empty($parsed)) {
                    throw new BadRequestException('No valid fields were found in record.');
                }

                if (!$builder->insert($parsed)) {
                    throw new InternalServerErrorException("Record insert failed.");
                }

                $idName = (isset($this->tableIdsInfo, $this->tableIdsInfo[0], $this->tableIdsInfo[0]->name))
                    ? $this->tableIdsInfo[0]->name : null;
                // take care to return auto created id values
                if (is_string($id) && $idName &&
                    ((0 === strcasecmp($id, 'uuid()')) || (0 === strcasecmp($id, 'now()')))
                ) {
                    if ($newId = Arr::get($parsed, $idName)) {
                        switch ($newId::class) {
                            case 'Cassandra\Uuid':
                            case 'Cassandra\Timeuuid': // construct( int $seconds )
                                $id = $newId->uuid();
                                break;
                        }
                    }
                }
                $out = (is_array($id)) ? $id : [$idName => $id];

                // add via record, so batch processing can retrieve extras
                if ($requireMore) {
                    parent::addToTransaction($id);
                }
                break;

            case Verbs::PUT:
            case Verbs::PATCH:
                if (!empty($updates)) {
                    $record = $updates;
                }

                // remove id from record
                $record = Arr::except($record, array_keys($match));

                $parsed = $this->parseRecord($record, $this->tableFieldsInfo, $ssFilters, true);
                if (!empty($parsed)) {
                    if (!empty($match) && $this->parent->upsertAllowed() && !$builder->exists()) {
                        if (!$builder->insert(array_merge($match, $parsed))) {
                            throw new InternalServerErrorException("Record upsert failed.");
                        }
                    } else {
                        $rows = $builder->update($parsed);
                        if (0 >= $rows) {
                            // could have just not updated anything, or could be bad id
                            if (empty($this->runQuery($this->transactionTable, $builder, $extras))) {
                                throw new NotFoundException("Record with identifier '" .
                                    print_r($id, true) .
                                    "' not found.");
                            }
                        }
                    }
                }

                if (!empty($relatedInfo)) {
                    // need the id back in the record
                    if (!empty($match)) {
                        $record = array_merge($record, $match);
                    }
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
                        $builder,
                        $extras
                    );
                    if (empty($result)) {
                        // bail, we know it isn't there
                        throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
                    }

                    $out = $result[0];
                }

                if (1 > $builder->delete()) {
                    // wasn't anything there to delete
                    throw new NotFoundException("Record with identifier '" . print_r($id, true) . "' not found.");
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

                $result = $this->runQuery($this->transactionTable, $builder, $extras);
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
        $dbConn = $this->parent->getConnection();
        if (empty($this->batchRecords) && empty($this->batchIds)) {
            if (0 < $dbConn->transactionLevel()) {
                $dbConn->commit();
            }

            return null;
        }

        $updates = Arr::get($extras, 'updates');
        $ssFilters = Arr::get($extras, 'ss_filters');
        $related = Arr::get($extras, 'related');
        $requireMore = array_get_bool($extras, 'require_more') || !empty($related);

        $builder = $dbConn->table($this->transactionTableSchema->internalName);

        /** @type ColumnSchema $idName */
        $idName = (isset($this->tableIdsInfo, $this->tableIdsInfo[0])) ? $this->tableIdsInfo[0] : null;
        if (empty($idName)) {
            throw new BadRequestException('No valid identifier found for this table.');
        }

        if (!empty($this->batchRecords)) {
            if (is_array($this->batchRecords[0])) {
                $temp = [];
                foreach ($this->batchRecords as $record) {
                    $temp[] = Arr::get($record, $idName->getName(true));
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
                $result = $this->runQuery($this->transactionTable, $builder, $extras);
                if (empty($result)) {
                    throw new NotFoundException('No records were found using the given identifiers.');
                }

                $out = $result;
            } else {
                $out = $this->retrieveRecords($this->transactionTable, $this->batchRecords, $extras);
            }

            $this->batchRecords = [];
        } elseif (!empty($this->batchIds)) {
            switch ($action) {
                case Verbs::PUT:
                case Verbs::PATCH:
                    if (!empty($updates)) {
                        $parsed = $this->parseRecord($updates, $this->tableFieldsInfo, $ssFilters, true);
                        if (!empty($parsed)) {
                            $rows = $builder->update($parsed);
                            if (count($this->batchIds) !== $rows) {
                                throw new BadRequestException('Batch Error: Not all requested records could be updated.');
                            }
                        }


                        if ($requireMore) {
                            $result = $this->runQuery(
                                $this->transactionTable,
                                $builder,
                                $extras
                            );

                            $out = $result;
                        }
                    }
                    break;

                case Verbs::DELETE:
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $builder,
                        $extras
                    );
                    if (count($this->batchIds) !== count($result)) {
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == Arr::get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    break;
                                }
                            }
                            if (!$found) {
                                $out[$index] = new NotFoundException("Record with identifier '" . print_r($id,
                                        true) . "' not found.");
                            }
                        }
                    } else {
                        $out = $result;
                    }

                    $rows = $builder->delete();
                    if (count($this->batchIds) !== $rows) {
                        throw new BatchException($out, 'Batch Error: Not all requested records could be deleted.');
                    }
                    break;

                case Verbs::GET:
                    $result = $this->runQuery(
                        $this->transactionTable,
                        $builder,
                        $extras
                    );

                    if (count($this->batchIds) !== count($result)) {
                        foreach ($this->batchIds as $index => $id) {
                            $found = false;
                            foreach ($result as $record) {
                                if ($id == Arr::get($record, $idName->getName(true))) {
                                    $out[$index] = $record;
                                    $found = true;
                                    break;
                                }
                            }
                            if (!$found) {
                                $out[$index] = new NotFoundException("Record with identifier '" . print_r($id,
                                        true) . "' not found.");
                            }
                        }

                        throw new BatchException($out, 'Batch Error: Not all requested records could be retrieved.');
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

        if (0 < $dbConn->transactionLevel()) {
            $dbConn->commit();
        }

        return $out;
    }

    /**
     * {@inheritdoc}
     */
    protected function rollbackTransaction()
    {
        // TODO: Implement rollbackTransaction() method.
    }
}