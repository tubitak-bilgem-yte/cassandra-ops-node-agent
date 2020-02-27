package tr.gov.tubitak.bilgem.yte.cassandraops.nodeagent.modules.backup;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

public class GetCassandraSchmeMetaDataTest {
	private final String cassandraPath = "C:\\Program Files\\DataStax-DDC\\apache-cassandra\\bin\\";
	private final String command = "\"DESC urunhareketleri;\"";

	//    @Test
	public void testSubprocess() {
		Path binPath = Paths.get(cassandraPath);
		Path pythonPath = binPath.getParent().getParent();
		String pythonPathString = "& \"" + pythonPath.toString() + "\\python\\python.exe" + "\"";
		String cqlshPath = "\"" + binPath + "\\cqlsh.py" + "\"";

		String wholeCommand = pythonPathString + " " + cqlshPath + " -e " + command;
		System.out.println(wholeCommand);
		try {
			Process process = Runtime.getRuntime().exec(wholeCommand);
			process.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
			StringBuilder output = new StringBuilder();
			String line;
			while ((line = reader.readLine()) != null) {
				output.append(line).append("\n");
			}
			System.out.println(output.toString());
		} catch (final IOException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		}
	}
}
