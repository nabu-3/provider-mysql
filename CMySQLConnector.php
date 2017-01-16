<?php

/*  Copyright 2009-2011 Rafael Gutierrez Martinez
 *  Copyright 2012-2013 Welma WEB MKT LABS, S.L.
 *  Copyright 2014-2016 Where Ideas Simply Come True, S.L.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

namespace providers\mysql;

use \nabu\core\CNabuEngine;
use \nabu\core\exceptions\ENabuCoreException;
use \nabu\db\CNabuDBAbstractConnector;

/**
 * MySQL implementation to use MySQL servers as database engine for Nabu 3
 * @author Rafael Gutiérrez <rgutierrez@wiscot.com>
 * @version 3.0.0 Surface
 * @package providers\mysql
 */
final class CMySQLConnector extends CNabuDBAbstractConnector
{
    /* Object types */
    const TYPE_TABLE = 'table';

    /* Warning codes */
    const WARNING_TABLE_ALREADY_EXISTS  = 1050;

    /**
     * Database connector handler that handles the connection or false if not connected
     * @var mixed
     */
    private $connector = false;

    /**
     * Syntax builder object.
     * @var CMySQLSyntaxBuilder
     */
    private $syntax_builder = null;

    /**
     * Statements associative array
     * @var array
     */
    private $statements = null;
    private $queries_executed;
    private $queries_released;
    private $queries_erroneous;
    private $sentences_executed;
    private $sentences_erroneous;
    private $inserts_executed;
    private $inserts_erroneous;
    private $updates_executed;
    private $updates_erroneous;
    private $deletes_executed;
    private $deletes_erroneous;

    public function __destruct()
    {
        while (count($this->statements) > 0) {
            $statement = array_pop($this->statements);
            $statement->release();
        }

        if ($this->isConnected()) {
            $this->disconnect();
        }
    }

    public function getDriverName()
    {
        return mysqli_get_client_info($this->connector);
    }

    public function getServerInfo()
    {
        if ($this->isConnected()) {
            return array(
                'host' => mysqli_get_host_info($this->connector),
                'protocol' => mysqli_get_proto_info($this->connector),
                'name' => 'MySQL ' . mysqli_get_server_info($this->connector),
                'version' => mysqli_get_server_version($this->connector)
            );
        }

        return false;
    }

    public function getSyntaxBuilder()
    {
        if ($this->syntax_builder === null) {
            $this->syntax_builder = new CMySQLSyntaxBuilder($this);
        }

        return $this->syntax_builder;
    }

    public function getDescriptorFromFile($filename)
    {
        return CMySQLDescriptor::createFromFile($this, $filename);
    }

    public function connect()
    {
        $nb_engine = CNabuEngine::getEngine();

        $nb_engine->traceLog(null, "===> connect MySQL " . $this->getTimestamp() . " <===");
        if (is_string($this->host) &&
            is_numeric($this->port) &&
            is_string($this->user) &&
            is_string($this->password) &&
            is_string($this->schema)
        ) {
            if ($this->isConnected() && !$this->disconnect()) {
                return false;
            }
            $this->clearStats();
            $this->clearErrors();
            $this->connector = mysqli_connect("$this->host:$this->port", $this->user, $this->password);

            if ($this->connector === false) {
                throw new EMySQLException(
                    EMySQLException::ERROR_NOT_CONNECTED,
                    mysqli_connect_errno(),
                    mysqli_connect_error()
                );
            } else {
                if (!mysqli_select_db($this->connector, $this->schema)) {
                    throw new EMySQLException(
                        EMySQLException::ERROR_SCHEMA_NOT_FOUND,
                        mysqli_errno($this->connector),
                        mysqli_error($this->connector)
                    );
                }
                if (is_string($this->charset) && !mysqli_set_charset($this->connector, $this->charset)) {
                    throw new EMySQLException(
                        EMySQLException::ERROR_QUERY_EXECUTION,
                        mysqli_errno($this->connector),
                        mysqli_error($this->connector)
                    );
                }
                return true;
            }
        }

        return false;
    }

    public function isConnected()
    {
        return $this->connector !== false;
    }

    public function disconnect()
    {
        $nb_engine = CNabuEngine::getEngine();

        $nb_engine->traceLog(null, '===> disconnect MySQL ' . $this->getTimestamp()
                . ' queries ['
                . ' executed (' . $this->queries_executed . ')'
                . ' released (' . $this->queries_released . ')'
                . ' erroneous (' . $this->queries_erroneous . ')'
                . ' ]'
                . ' sentences ['
                . ' executed (' . $this->sentences_executed . ')'
                . ' erroneous (' . $this->sentences_erroneous . ')'
                . ' ]'
                . ' inserts ['
                . ' executed (' . $this->inserts_executed . ')'
                . ' erroneous (' . $this->inserts_erroneous . ')'
                . ' ]'
                . ' updates ['
                . ' executed (' . $this->updates_executed . ')'
                . ' erroneous (' . $this->updates_erroneous . ')'
                . ' ]'
                . ' deletes ['
                . ' executed (' . $this->deletes_executed . ')'
                . ' erroneous (' . $this->deletes_erroneous . ')'
                . ' ]'
                . ' <===');
        if ($this->connector !== false && is_object($this->connector) && !mysqli_close($this->connector)) {
            $this->setError(mysqli_error($this->connector), mysqli_errno($this->connector));
            return false;
        } else {
            $this->connector = false;
        }

        return true;
    }

    private function clearStats()
    {
        $this->queries_executed = 0;
        $this->queries_released = 0;
        $this->queries_erroneous = 0;

        $this->sentences_executed = 0;
        $this->sentences_erroneous = 0;

        $this->inserts_executed = 0;
        $this->inserts_erroneous = 0;

        $this->updates_executed = 0;
        $this->updates_erroneous = 0;

        $this->deletes_executed = 0;
        $this->deletes_erroneous = 0;
    }

    public function enqueueStatement($statement)
    {
        if ($statement instanceof CMySQLStatement) {
            $hash = $statement->getHash();
            if (count($this->statements) === 0) {
                $this->statements = array($hash => $statement);
            } else {
                $this->statements[$hash] = $statement;
            }
        }
    }

    public function dequeueStatement($statement)
    {
        if ($statement instanceof CMySQLStatement) {
            $hash = $statement->getHash();
            if ($hash !== null && count($this->statements) > 0 && array_key_exists($hash, $this->statements)) {
                unset($this->statements[$hash]);
            }
        }
    }

    public function setCharset($charset)
    {
        if ($this->isConnected() && !mysqli_set_charset($this->charset, $this->connector)) {
            $this->setError(mysqli_error($this->connector), mysqli_errno($this->connector));
            return false;
        }
        return parent::setCharset($charset);
    }

    public function setSchema($schema)
    {
        if ($this->isConnected() && !mysqli_select_db($this->schema, $this->connector)) {
            $this->setError(mysqli_error($this->connector), mysqli_errno($this->connector));
            return false;
        }
        return parent::setSchema($schema);
    }

    public function testConnection()
    {
        return $this->getQueryAsSingleField('counter', 'select 1 as counter from dual') == 1;
    }

    public function buildSentence($sentence, $params)
    {
        if (is_string($sentence) && strlen($sentence) > 0 && is_array($params)) {
            $final_params = array();
            foreach ($params as $key => $value) {
                if (is_string($value)) {
                    $final_params[$key] = mysqli_real_escape_string($this->connector, $value);
                } else {
                    $final_params[$key] = $value;
                }
            }
            return nb_vnsprintf($sentence, $final_params);
        } else {
            return $sentence;
        }
    }

    public function implodeStringArray($array, $glue)
    {
        if (count($array) === 0) {
            return "";
        }

        $imploded = "";
        foreach ($array as $item) {
            $imploded .= (strlen($imploded) === 0 ? '' : $glue)
                      . "'"
                      . mysqli_real_escape_string($this->connector, $item)
                      . "'";
        }

        return $imploded;
    }

    public function implodeIntegerArray($array, $glue)
    {
        if (count($array) === 0) {
            return "";
        }

        $imploded = "";
        foreach ($array as $item) {
            $imploded .= (strlen($imploded) === 0 ? '' : $glue) . mysqli_real_escape_string($this->connector, $item);
        }

        return $imploded;
    }

    public function getQuery($sentence, $params = null, $trace = false)
    {
        $nb_engine = CNabuEngine::getEngine();

        if (!$this->isConnected()) {
            $this->queries_erroneous++;
            throw new EMySQLException(EMySQLException::ERROR_NOT_CONNECTED);
        }

        if (!is_string($sentence) || strlen($sentence) === 0) {
            $this->queries_erroneous++;
            throw new EMySQLException(EMySQLException::ERROR_QUERY_INVALID_SENTENCE);
        }

        $final_sentence = $this->buildSentence($sentence, $params);

        if ($trace === true) {
            $nb_engine->errorLog("Query: $final_sentence", E_CORE_WARNING);
        } elseif ($this->trace) {
            $nb_engine->traceLog(null, "Query: $final_sentence", E_CORE_WARNING);
        }

        $this->analyzeDuplicatedQuery($final_sentence);

        $result = mysqli_query($this->connector, $final_sentence);

        if ($result === false || mysqli_errno($this->connector) > 0) {
            $this->queries_erroneous++;
            throw new EMySQLException(
                EMySQLException::ERROR_QUERY_EXECUTION,
                mysqli_errno($this->connector),
                mysqli_error($this->connector),
                $final_sentence
            );
        } elseif ($result === true) {
            $this->queries_erroneous++;
            throw new EMySQLException(EMySQLException::ERROR_QUERY_RESULTSET_NOT_FOUND);
        } else {
            $this->queries_executed++;
            return new CMySQLStatement($this, $result);
        }
    }

    public function releaseStatement($statement)
    {
        if (is_resource($statement)) {
            mysqli_free_result($statement);
            $this->queries_released++;
        }
    }

    public function executeSentence($sentence, $params = null, $trace = false)
    {
        $nb_engine = CNabuEngine::getEngine();

        if (!$this->isConnected()) {
            $this->queries_erroneous++;
            throw new EMySQLException(EMySQLException::ERROR_NOT_CONNECTED);
        }

        if (!is_string($sentence) || strlen($sentence) === 0) {
            $this->queries_erroneous++;
            throw new EMySQLException(EMySQLException::ERROR_QUERY_INVALID_SENTENCE);
        }

        $final_sentence = $this->buildSentence($sentence, $params);

        if ($trace === true) {
            $nb_engine->errorLog("Query: $final_sentence", E_CORE_WARNING);
        } elseif ($this->trace) {
            $nb_engine->traceLog(null, "Query: $final_sentence", E_CORE_WARNING);
        }

        $result = mysqli_query($this->connector, $final_sentence);

        if ($result === false || mysqli_errno($this->connector) > 0) {
            $this->sentences_erroneous++;
            throw new EMySQLException(
                EMySQLException::ERROR_QUERY_EXECUTION,
                mysqli_errno($this->connector),
                mysqli_error($this->connector),
                $final_sentence
            );
        } elseif ($result !== true) {
            $this->sentences_erroneous++;
            throw new EMySQLException(EMySQLException::ERROR_QUERY_RESULTSET_NOT_ALLOWED);
        } else {
            $this->sentences_executed++;
            return true;
        }
    }


    public function executeInsert($sentence, $params = null, $trace = false)
    {
        $nb_engine = CNabuEngine::getEngine();

        if (!$this->isConnected()) {
            $this->updates_erroneous++;
            throw new EMySQLException(EMySQLException::ERROR_NOT_CONNECTED);
        }

        if (!is_string($sentence) || strlen($sentence) === 0) {
            $this->updates_erroneous++;
            throw new EMySQLException(EMySQLException::ERROR_QUERY_INVALID_SENTENCE);
        }

        $final_sentence = $this->buildSentence($sentence, $params);

        if ($trace === true) {
            $nb_engine->errorLog("Query: $final_sentence", E_CORE_WARNING);
        } elseif ($this->trace) {
            $nb_engine->traceLog(null, "Query: $final_sentence", E_CORE_WARNING);
        }

        $this->analyzeDuplicatedQuery($final_sentence);

        $result = mysqli_query($this->connector, $final_sentence);

        if ($result === false || mysqli_errno($this->connector) > 0) {
            $this->updates_erroneous++;
            throw new EMySQLException(
                EMySQLException::ERROR_QUERY_EXECUTION,
                mysqli_errno($this->connector),
                mysqli_error($this->connector),
                $final_sentence
            );
        }

        $this->updates_executed++;

        return $this->getAffectedRows();
    }

    public function executeUpdate($sentence, $params = null, $trace = false)
    {
        return $this->executeInsert($sentence, $params, $trace);
    }

    public function executeDelete($sentence, $params = null, $trace = false)
    {
        return $this->executeInsert($sentence, $params, $trace);
    }

    public function checkForWarning($code)
    {
        if (mysqli_warning_count($this->connector) > 0) {
            $warning = mysqli_get_warnings($this->connector);
            do {
                if ($warning->errno === $code) {
                    return true;
                }
            } while ($warning->next());
        }

        return false;
    }

    public function getDoc($sentence, $trace = false)
    {
        throw new ENabuCoreException(ENabuCoreException::ERROR_METHOD_NOT_IMPLEMENTED);
    }

    public function getQueryAsSingleRow($sentence, $params = false, $trace = false)
    {
        if (is_string($sentence)) {
            $statement = $this->getQuery($sentence, $params, $trace);
            if ($statement) {
                if ($statement->getRowsCount() > 0) {
                    $row = $statement->fetchAsAssoc();
                    $statement->release();
                    return (count($row) > 0 ? $row : null);
                } else {
                    $statement->release();
                    return null;
                }
            }
        }

        return false;
    }

    public function getQueryAsSingleField($field, $sentence, $params = false, $trace = false)
    {
        if (is_string($sentence) && is_string($field)) {
            $statement = $this->getQuery($sentence, $params, $trace);
            if ($statement) {
                $value = false;
                if (($row = $statement->fetchAsAssoc()) && array_key_exists($field, $row)) {
                    $value = $row[$field];
                }
                $statement->release();
                return $value;
            }
        }

        return false;
    }

    public function getQueryAsArray($sentence, $params = false, $trace = false)
    {
        $statement = $this->getQuery($sentence, $params, $trace);
        if ($statement) {
            for ($list = array(); ($row = $statement->fetchAsAssoc()) !== null;) {
                $list[] = $row;
            }
            $statement->release();
            return (count($list) > 0 ? $list : null);
        }

        return false;
    }

    public function getQueryAsAssoc($index_field, $sentence, $params = false, $trace = false)
    {
        $statement = $this->getQuery($sentence, $params, $trace);
        if ($statement) {
            for ($list = array(); ($row = $statement->fetchAsAssoc()) !== null;) {
                if (array_key_exists($index_field, $row)) {
                    $list[$row[$index_field]] = $row;
                }
            }
            $statement->release();
            return (count($list) > 0 ? $list : null);
        }

        return false;
    }

    public function getQueryAsArrayOfSingleField($field, $sentence, $params = false, $trace = false)
    {
        $statement = $this->getQuery($sentence, $params, $trace);
        if ($statement) {
            for ($list = array(); ($row = $statement->fetchAsAssoc()) !== null;) {
                if (array_key_exists($field, $row)) {
                    $list[] = $row[$field];
                }
            }
            $statement->release();
            return (count($list) > 0 ? $list : null);
        }

        return false;
    }

    public function getQueryAsAssocPairedFields($index_field, $value_field, $sentence, $params = false, $trace = false)
    {
        $statement = $this->getQuery($sentence, $params, $trace);
        if ($statement) {
            for ($list = array(); ($row = $statement->fetchAsAssoc()) !== null;) {
                if (array_key_exists($index_field, $row) && array_key_exists($value_field, $row)) {
                    $list[$row[$index_field]] = $row[$value_field];
                }
            }
            $statement->release();
            return (count($list) > 0 ? $list : null);
        }

        return false;
    }

    public function getQueryAsCount($table, $where = null, $distinct = null, $trace = false)
    {
        $count = 0;

        if (is_string($table)) {
            $statement = $this->getQuery(
                "select count(".(is_string($distinct) ? "distinct $distinct" : "*").") "
                . "from $table"
                . (is_string($where) ? " where $where" : ""), null, $trace
            );
            if ($statement) {
                if (($row = $statement->fetchAsArray())) {
                    $count = (int)$row[0];
                }
                $statement->release();
                return $count;
            }
        }

        return false;
    }

    public function getQueryAsObject($classname, $sentence, $params = null, $trace = false)
    {
        if (is_string($sentence) && is_string($classname)) {
            $statement = $this->getQuery($sentence, $params, $trace);
            if ($statement) {
                $object = null;
                if ($statement->getRowsCount() > 0) {
                    $object = new $classname();
                    if (!$object->fetch($statement)) {
                        $object = null;
                    }
                }
                $statement->release();

                return $object;
            }
        }

        return false;
    }

    public function getQueryAsObjectWithSubClassing(
        $subclassing_field,
        $query,
        $params = null,
        $subclassing_default = null,
        $trace = false
    ) {
        if (is_string($subclassing_field)) {
            $statement = $this->getQuery($query, $params, $trace);
            if ($statement) {
                $build = null;
                if ($statement->getRowsCount() > 0) {
                    $data = $statement->fetchAsAssoc();
                    if (array_key_exists($subclassing_field, $data)) {
                        if (strlen($data[$subclassing_field]) > 0) {
                            $build = new $data[$subclassing_field];
                        } elseif (strlen($subclassing_default) > 0) {
                            $build = new $subclassing_default;
                        } else {
                            return null;
                        }
                        $build->copyData($data);
                        if (!$build->fill()) {
                            $build = null;
                        }
                    }
                }
                $statement->release();
                return $build;
            }
        }

        return false;
    }

    public function getQueryAsObjectArray($classname, $index_field, $sentence, $params = null, $trace = false)
    {
        if (is_string($classname)) {
            if (($statement = $this->getQuery($sentence, $params, $trace))) {
                $list = array();
                do {
                    $object = new $classname();
                    if ($object->fetch($statement)) {
                        if ($index_field === null) {
                            $list[] = $object;
                        } else {
                            $list[$object->getValue($index_field)] = $object;
                        }
                    }
                } while ($object->isFetched());
                $statement->release();
                return (count($list) > 0 ? $list : null);
            }
        }

        return false;
    }

    public function getQueryAsObjectArrayWithSubclassing(
        $index_field,
        $subclassing_field,
        $query,
        $params = null,
        $subclassing_default = null,
        $trace = false
    ) {
        if (is_string($subclassing_field)) {
            if (($statement = $this->getQuery($query, $params, $trace))) {
                $list = array();
                while ($data = $statement->fetchAsAssoc()) {
                    if (array_key_exists($subclassing_field, $data) && strlen($data[$subclassing_field]) > 0) {
                        $build = new $data[$subclassing_field];
                    } elseif (strlen($subclassing_default) > 0) {
                        $build = new $subclassing_default;
                    } else {
                        $list = false;
                        break;
                    }

                    if ($build !== null) {
                        $build->copyData($data);
                        if ($build->fill()) {
                            if ($index_field === null) {
                                $list[] = $build;
                            } else {
                                $list[$build->getValue($index_field)] = $build;
                            }
                        }
                    }
                }
                if ($list !== false && count($list) === 0) {
                    $list = null;
                }
                $statement->release();
                return $list;
            }
        }

        return false;
    }

    public function getLastInsertedId()
    {
        return ($this->connector ? mysqli_insert_id($this->connector) : false);
    }

    public function getAffectedRows()
    {
        return ($this->connector ? mysqli_affected_rows($this->connector) : false);
    }

    public function beginTransaction($trace = false)
    {
        return ($this->executeSentence("START TRANSACTION", null, $trace) !== false) &&
               ($this->executeSentence("BEGIN", null, $trace) !== false);
    }

    public function commitTransaction($trace = false)
    {
        return $this->executeSentence("COMMIT", null, $trace) !== false;
    }

    public function rollbackTransaction($trace = false)
    {
        return $this->executeSentence("ROLLBACK", null, $trace) !== false;
    }

    public function deleteDoc($sentence, $trace = false)
    {
        throw new ENabuCoreException(ENabuCoreException::ERROR_METHOD_NOT_IMPLEMENTED);
    }
}
