/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.query;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.table.HudiTableFileSystemView;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;

public abstract class AbstractDirectoryLister
        implements HudiDirectoryLister
{
    protected final HudiTableFileSystemView fileSystemView;
    protected final List<Column> partitionColumns;

    protected final List<HudiPartitionInfo> allPartitionInfoList;

    protected final HudiTableHandle tableHandle;

    protected final HiveMetastore hiveMetastore;

    protected final Table hiveTable;

    protected final TypeManager typeManager;

    protected boolean initPartitionInfo;

    public AbstractDirectoryLister(
            HudiTableHandle tableHandle,
            HudiTableFileSystemView fileSystemView,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            TypeManager typeManager)
    {
        this.tableHandle = tableHandle;
        this.partitionColumns = hiveTable.getPartitionColumns();
        this.hiveMetastore = hiveMetastore;
        this.hiveTable = hiveTable;
        this.typeManager = typeManager;
        this.fileSystemView = fileSystemView;
        this.allPartitionInfoList = getAllPartitionValues().stream()
                .map(hivePartitionName -> new HiveHudiPartitionInfo(
                        hivePartitionName,
                        partitionColumns,
                        this))
                .collect(Collectors.toList());
    }

    @Override
    public Optional<HudiPartitionInfo> getPartitionInfo(String partition)
    {
        return allPartitionInfoList.stream()
                .filter(partitionInfo -> partition.equals(partitionInfo.getHivePartitionName()))
                .findFirst();
    }

    @Override
    public void close()
    {
        if (fileSystemView != null && !fileSystemView.isClosed()) {
            fileSystemView.close();
        }
    }

    @Override
    public List<String> getAllPartitionValues()
    {
        if (initPartitionInfo) {
            return allPartitionInfoList.stream().map(item -> item.getHivePartitionName()).collect(Collectors.toList());
        }

        Optional<Table> table = hiveMetastore.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        verify(table.isPresent());
        List<Column> partitionColumns = table.get().getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of("");
        }

        List<String> allPartitions = getPartitionsInner();
        initPartitionInfo = true;
        return allPartitions;
    }

    public abstract List<String> getPartitionsInner();
}
