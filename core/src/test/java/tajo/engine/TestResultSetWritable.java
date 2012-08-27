/**
 * 
 */
package tajo.engine;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.catalog.Schema;
import tajo.catalog.TCatUtil;
import tajo.catalog.TableMeta;
import tajo.catalog.proto.CatalogProtos.DataType;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.common.type.IPv4;
import tajo.conf.NtaConf;
import tajo.datum.DatumFactory;
import tajo.storage.Appender;
import tajo.storage.StorageManager;
import tajo.storage.Tuple;
import tajo.storage.VTuple;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author jimin
 * 
 */
public class TestResultSetWritable {

	private NtaConf conf;
	private static String TEST_PATH = "target/test-data/TestResultSetWritable";
	private ResultSetWritable result;
	private IPv4[] ips;
	private StorageManager sm;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		conf = new NtaConf();
		WorkerTestingUtil.buildTestDir(TEST_PATH);
		sm = StorageManager.get(conf, TEST_PATH);
		
		Schema schema = new Schema();
		schema.addColumn("name", DataType.STRING);
		schema.addColumn("id", DataType.INT);
		schema.addColumn("ip", DataType.IPv4);

		TableMeta desc = TCatUtil.newTableMeta(schema, StoreType.RAW);

		Appender appender = sm.getTableAppender(desc, "table1");

		ips = new IPv4[4];
		for (int i = 1; i <= ips.length; i++) {
			ips[i - 1] = new IPv4("163.152.161." + i);
		}

		VTuple t1 = new VTuple(3);
		t1.put(0, DatumFactory.createString("hyunsik"));
		t1.put(1, DatumFactory.createInt(1));
		t1.put(2, DatumFactory.createIPv4(ips[0].getBytes()));
		appender.addTuple(t1);

		VTuple t2 = new VTuple(3);
		t2.put(0, DatumFactory.createString("jihoon"));
		t2.put(1, DatumFactory.createInt(2));
		t2.put(2, DatumFactory.createIPv4(ips[1].getBytes()));
		appender.addTuple(t2);

		VTuple t3 = new VTuple(3);
		t3.put(0, DatumFactory.createString("jimin"));
		t3.put(1, DatumFactory.createInt(3));
		t3.put(2, DatumFactory.createIPv4(ips[2].getBytes()));
		appender.addTuple(t3);

		VTuple t4 = new VTuple(3);
		t4.put(0, DatumFactory.createString("haemi"));
		t4.put(1, DatumFactory.createInt(4));
		t4.put(2, DatumFactory.createIPv4(ips[3].getBytes()));
		appender.addTuple(t4);
		appender.close();

		result = new ResultSetWritable();
		result.setResult(sm.getTablePath("table1"));
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testNext() throws IOException {
		Tuple tuple = null;
		
		tuple = result.next();
		assertNotNull(tuple);
		assertEquals("hyunsik",tuple.getString(0).asChars());
		assertEquals(1,tuple.getInt(1).asInt());
		assertTrue(Arrays.equals(ips[0].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = result.next();
		assertNotNull(tuple);
		assertEquals("jihoon",tuple.getString(0).asChars());
		assertEquals(2,tuple.getInt(1).asInt());
		assertTrue(Arrays.equals(ips[1].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = result.next();
		assertNotNull(tuple);
		assertEquals("jimin",tuple.getString(0).asChars());
		assertEquals(3,tuple.getInt(1).asInt());
		assertTrue(Arrays.equals(ips[2].getBytes(), tuple.getIPv4Bytes(2)));
		
		tuple = result.next();
		assertNotNull(tuple);
		assertEquals("haemi",tuple.getString(0).asChars());
		assertEquals(4,tuple.getInt(1).asInt());
		assertTrue(Arrays.equals(ips[3].getBytes(), tuple.getIPv4Bytes(2)));
	}
}