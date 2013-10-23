package in.naishe.mc.extra;

import in.naishe.mc.Setup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.model.BasicKeyspaceDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.cassandra.utils.ByteBufferUtil;

import com.google.common.base.Strings;

public class LoadData {

	public static void main(String[] args) throws IOException {
		loadData(Setup.DATA_FILE);
	}

	private static void loadData(String filePath) throws IOException {

		// Step 1: Setup Cassandra
		CassandraHostConfigurator conf = new CassandraHostConfigurator(Setup.CASSANDRA_HOST_ADDR + ":" + Setup.CASSANDRA_RPC_PORT);
		Cluster cluster = HFactory.getOrCreateCluster(Setup.CASSANDRA_CLUSTER, conf);

		createSchemaUsingThrift(cluster);

		Keyspace keyspace = HFactory.createKeyspace(Setup.KEYSPACE, cluster);
		StringSerializer stringSerializer = StringSerializer.get();
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);

		// Step 2: load the data
		InputStream fileStream = LoadData.class.getResourceAsStream(filePath);
		InputStreamReader reader = new InputStreamReader(fileStream);
		BufferedReader br = new BufferedReader(reader);
		int lineNo = 0;
		String line;
		while ((line = br.readLine()) != null) {
			if (Strings.isNullOrEmpty(line))
				continue;

			String rowKey = "row_" + (int) (lineNo / 500);
			String colName = "col_" + (lineNo % 500);
			// Quick & Dirty remove punctuations
			line = line.replaceAll("(\\w+)\\p{Punct}(\\s|$)", "$1$2");
			System.out.println("Inserting: [" + rowKey + " = { " + colName + ": " + line + " }]");
			mutator.insert(rowKey, Setup.INPUT_CF, HFactory.createStringColumn(colName, line));
			lineNo++;
		}
		br.close();
		reader.close();
		fileStream.close();
		// The cluster should typically live for the life of your application.
		HFactory.shutdownCluster(cluster);
		System.out.println(" --- DATA INSERTION COMPLETED --- ");
	}

	private static void createSchemaUsingThrift(Cluster cluster) {
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(Setup.KEYSPACE);

		// If keyspace does not exist, the CFs don't exist either. => create
		// them.
		if (keyspaceDef != null) {
			System.out.println("Deleting existing keyspace: " + Setup.KEYSPACE);
			cluster.dropKeyspace(Setup.KEYSPACE);
			System.out.println("Creating schema equivalent of:");
			System.out.println("Keyspace does not exit: " + Setup.KEYSPACE);
			System.out.println("Please create the keyspace and CF from cassandra-cli as follows: ");
			System.out.println("" 
					+ "create keyspace testks with placement_strategy = SimpleStrategy and strategy_options = {replication_factor: 1};\n\n" 
					+ "use testks;\n\n"
					+ "CREATE COLUMN FAMILY dataCF WITH comparator = UTF8Type AND key_validation_class=UTF8Type AND default_validation_class = UTF8Type;\n\n"
					+ "CREATE COLUMN FAMILY resultCF WITH comparator = UTF8Type AND key_validation_class=UTF8Type AND default_validation_class = IntegerType;\n\n"
					+ "CREATE COLUMN FAMILY result1CF WITH comparator = UTF8Type AND key_validation_class=UTF8Type AND default_validation_class = IntegerType;\n\n" 
					+ "");
		}

		System.out.println("Creating keyspace: " + Setup.KEYSPACE);
		ColumnFamilyDefinition cfin = new BasicColumnFamilyDefinition();
		cfin.setKeyspaceName(Setup.KEYSPACE);
		cfin.setName(Setup.INPUT_CF);
		cfin.setColumnType(ColumnType.STANDARD);
		cfin.setComparatorType(ComparatorType.UTF8TYPE);
		cfin.setKeyValidationClass(ComparatorType.UTF8TYPE.getTypeName());
		cfin.setDefaultValidationClass(ComparatorType.UTF8TYPE.getTypeName());

		BasicColumnDefinition col = new BasicColumnDefinition();
		col.setValidationClass(ComparatorType.INTEGERTYPE.getTypeName());
		col.setName(ByteBufferUtil.bytes("count"));
		
		ColumnFamilyDefinition cfout = new BasicColumnFamilyDefinition();
		cfout.setKeyspaceName(Setup.KEYSPACE);
		cfout.setName(Setup.OUTPUT_CF);
		cfout.setColumnType(ColumnType.STANDARD);
		cfout.setComparatorType(ComparatorType.UTF8TYPE);
		cfout.setKeyValidationClass(ComparatorType.UTF8TYPE.getTypeName());
		cfout.setDefaultValidationClass(ComparatorType.INTEGERTYPE.getTypeName());
		cfout.addColumnDefinition(col);

		ColumnFamilyDefinition cfout2 = new BasicColumnFamilyDefinition();
		cfout2.setKeyspaceName(Setup.KEYSPACE);
		cfout2.setName(Setup.OUTPUT2_CF);
		cfout2.setColumnType(ColumnType.STANDARD);
		cfout2.setComparatorType(ComparatorType.UTF8TYPE);
		cfout2.setKeyValidationClass(ComparatorType.UTF8TYPE.getTypeName());
		cfout2.setDefaultValidationClass(ComparatorType.INTEGERTYPE.getTypeName());
		cfout2.addColumnDefinition(col);

		BasicKeyspaceDefinition ks = new BasicKeyspaceDefinition();
		ks.setName(Setup.KEYSPACE);
		ks.setReplicationFactor(1);
		ks.setStrategyClass("SimpleStrategy");
		ks.addColumnFamily(new ThriftCfDef(cfin));
		ks.addColumnFamily(new ThriftCfDef(cfout));
		ks.addColumnFamily(new ThriftCfDef(cfout2));
		cluster.addKeyspace(ks);

	}
}
