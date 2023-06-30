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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.files.FileSlice;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.table.HudiTableFileSystemView;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.type.TypeManager;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeyColumnHandles;
import static io.trino.plugin.hudi.util.HudiUtil.buildPartitionKeys;
import static java.lang.String.format;

public class HudiReadOptimizedDirectoryLister
        extends AbstractDirectoryLister
{
    public HudiReadOptimizedDirectoryLister(
            HudiTableHandle tableHandle,
            HudiTableFileSystemView fileSystemView,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            TypeManager typeManager)
    {
        super(tableHandle, fileSystemView, hiveMetastore, hiveTable, typeManager);
    }

    @Override
    public List<FileSlice> listFileSlice(HudiPartitionInfo partitionInfo, String commitTime)
    {
        return fileSystemView.getLatestFileSlicesBeforeOrOn(
                        partitionInfo.getRelativePartitionPath(),
                        commitTime,
                        false)
                .collect(toImmutableList());
    }

    @Override
    public List<String> getPartitionsInner()
    {
        List<HiveColumnHandle> partitionColumnHandles = getPartitionKeyColumnHandles(hiveTable, typeManager);

        return hiveMetastore.getPartitionNamesByFilter(
                        tableHandle.getSchemaName(),
                        tableHandle.getTableName(),
                        partitionColumns.stream().map(Column::getName).collect(Collectors.toList()),
                        computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates()))
                .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
    }

    public String getRelativePartitionPath(String hivePartitionName)
    {
        Optional<Partition> partition = hiveMetastore.getPartition(hiveTable, HiveUtil.toPartitionValues(hivePartitionName));
        if (partition.isEmpty()) {
            throw new HudiIOException(format("Cannot find partition in Hive Metastore: %s", hivePartitionName));
        }
        return FSUtils.getRelativePartitionPath(
                new Path(hiveTable.getStorage().getLocation()),
                new Path(partition.get().getStorage().getLocation()));
    }

    public List<HivePartitionKey> getHivePartitionKeys(String hivePartitionName)
    {
        Optional<Partition> partition = hiveMetastore.getPartition(hiveTable, HiveUtil.toPartitionValues(hivePartitionName));
        if (partition.isEmpty()) {
            throw new HudiIOException(format("Cannot find partition in Hive Metastore: %s", hivePartitionName));
        }
        return buildPartitionKeys(partitionColumns, partition.get().getValues());
    }
}
