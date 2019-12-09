import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.PriorityQueue;

public class ExternalMemoryImpl extends IExternalMemory {

	// block size in bytes
	private static final int BLOCK_SIZE = 4 * 1000;
	// maximum number of bytes in main memory
	private static final int MAX_MEMORY = 30 * 1000000;

	private class LineRef implements Comparable<LineRef> {

		private String line;
		private int bufferNum;

		LineRef(String line, int bufferNum) {
			this.line = line;
			this.bufferNum = bufferNum;
		}

		public String getLine() {
			return line;
		}

		public int getBufferNum() {
			return bufferNum;
		}

		public void setLine(String line) {
			this.line = line;
		}

		@Override
		public int compareTo(LineRef other) {
			return this.line.compareTo(other.line);
		}
	}

	@Override
	public void sort(String in, String out, String tmpPath) {
		try {
			twoPhaseSort(in, out, tmpPath, null, false);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void twoPhaseSort(String in, String out, String tmpPath, String subStr, boolean select)
			throws IOException {

		// Initial calculations:
		FileReader input = new FileReader(in);
		BufferedReader buffer = new BufferedReader(input);
		String line = buffer.readLine();
		int lineSize = line.length() * 2;  // every char is 2 bytes
		int counter = 0;
		int tmpFileCounter = 0;
		int maxLinesMemory = MAX_MEMORY / lineSize;
		String[] lines = new String[maxLinesMemory];

		// Phase I:
		while(line != null){
			if(counter == maxLinesMemory){
				sortAndWriteTmp(lines, makeTmpFilePath(tmpPath, tmpFileCounter++), maxLinesMemory);
				lines = new String[maxLinesMemory];
				counter = 0;
			}
			if(!select || checkSelect(line, subStr)) {
				lines[counter++] = line;
			}
			line = buffer.readLine();
		}
		sortAndWriteTmp(lines, makeTmpFilePath(tmpPath, tmpFileCounter++), counter);

		// Phase II:
		BufferedReader[] pointers = new BufferedReader[tmpFileCounter];

		for (int i = 0; i < tmpFileCounter; i++) {
			File file = new File(makeTmpFilePath(tmpPath, i));
			file.deleteOnExit();
			pointers[i] = new BufferedReader(new FileReader(file));
		}
		mergeSortedFiles(out, tmpFileCounter, pointers);

		// Closing:
		for (int i = 0; i < tmpFileCounter; i++) {
			pointers[i].close();
		}
		buffer.close();
	}

	private void mergeSortedFiles(String out, int tmpFileCounter, BufferedReader[] pointers) throws IOException {
		String line;BufferedWriter output = new BufferedWriter(new FileWriter(out));
		PriorityQueue<LineRef> minHeap = new PriorityQueue<>();

		// Add to the heap LineRef with first line from every temp file
		for (int i = 0; i < tmpFileCounter; i++) {
			minHeap.add(new LineRef(pointers[i].readLine(), i));
		}

		while (!minHeap.isEmpty()) {
			LineRef minLine = minHeap.remove();
			output.write(minLine.getLine() + '\n');

			if ((line = pointers[minLine.getBufferNum()].readLine()) != null) {
				minLine.setLine(line);
				minHeap.add(minLine);
			}
		}
		output.close();
	}

	private String makeTmpFilePath(String relativePath, int num){
		return Paths.get(relativePath, "tmp" + num).toString();
	}

	private void sortAndWriteTmp(String[] linesToSort, String tmpPath, int len) throws IOException {
		Arrays.sort(linesToSort, 0, len);
		BufferedWriter writer = new BufferedWriter(new FileWriter(tmpPath));
		for (int i=0; i < len; i++) {
			writer.write(linesToSort[i] + '\n');
		}
		writer.close();
	}

	@Override
	protected void join(String in1, String in2, String out, String tmpPath) {
		try {
			joinTwoSorted(in1, in2, out, tmpPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void joinTwoSorted(String in1, String in2, String out, String tmpPath) throws IOException {
		FileReader sorted1 = new FileReader(in1);
		FileReader sorted2 = new FileReader(in2);
		BufferedWriter output = new BufferedWriter(new FileWriter(out));

		BufferedReader buffer1 = new BufferedReader(sorted1);
		BufferedReader buffer2 = new BufferedReader(sorted2);

		String line1 = buffer1.readLine();
		String line2 = buffer2.readLine();
		String id1 = line1.split("\\s+")[0];
		String id2 = line2.split("\\s+")[0];

		while(line1 != null && line2 != null){
			while(id1.compareTo(id2) > 0){ // id1 > id2
				line2 = buffer2.readLine();
				if(line2 != null) {
					id2 = line2.split("\\s+")[0];
				}
				else break;
			}
			while(id1.compareTo(id2) < 0){ // id1 < id2
				line1 = buffer1.readLine();
				if(line1 != null) {
					id1 = line1.split("\\s+")[0];
				}
				else break;
			}
			while(line1 != null && line2 != null && id1.compareTo(id2) == 0){ // id1 = id2
				while(id1.compareTo(id2) == 0){
					output.write(combineLines(line1, line2));
					line2 = buffer2.readLine();
					if(line2 != null) {
						id2 = line2.split("\\s+")[0];
					}
					else break;
				}
				line1 = buffer1.readLine();
				if(line1 != null) {
					id1 = line1.split("\\s+")[0];
				}
				else break;
			}
		}

		output.close();
		buffer1.close();
		buffer2.close();
	}

	private String combineLines(String line1, String line2){
		String[] split = line2.split("\\s+");
		String[] withoutId = {split[1], split[2]};
		return line1 + " " + String.join(" ", withoutId) + '\n';
	}

	@Override
	protected void select(String in, String out, String substrSelect, String tmpPath) {
		try {
			simpleSelect(in, out, substrSelect);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void simpleSelect(String in, String out, String substrSelect) throws IOException {
		FileReader input = new FileReader(in);
		BufferedReader buffer = new BufferedReader(input);
		BufferedWriter output = new BufferedWriter(new FileWriter(out));

		String line = buffer.readLine();
		while(line != null){
			if(checkSelect(line, substrSelect)){
				output.write(line + '\n');
			}
			line = buffer.readLine();
		}

		buffer.close();
		output.close();
	}

	private boolean checkSelect(String line, String substrSelect){
		String fullId = line.split("\\s+")[0];
		int id = Integer.parseInt(fullId.split("id")[1]);
		return Integer.toString(id).contains(substrSelect);
	}

	@Override
	public void joinAndSelectEfficiently(String in1, String in2, String out,
			String substrSelect, String tmpPath) {
		try {
			selectAndSort(in1, in2, out, substrSelect, tmpPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void selectAndSort(String in1, String in2, String out, String substrSelect,
							   String tmpPath) throws IOException {
		Path tmpPath1 = Paths.get(tmpPath, "sortSelected1.txt");
		twoPhaseSort(in1, tmpPath1.toString(), tmpPath, substrSelect, true);

		Path tmpPath2 = Paths.get(tmpPath, "sortSelected2.txt");
		twoPhaseSort(in2, tmpPath2.toString(), tmpPath, substrSelect, true);

		join(tmpPath1.toString(), tmpPath2.toString(), out, tmpPath);

		Files.deleteIfExists(tmpPath1);
		Files.deleteIfExists(tmpPath2);
	}

}