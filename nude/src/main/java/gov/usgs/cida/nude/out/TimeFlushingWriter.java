package gov.usgs.cida.nude.out;

import java.io.IOException;
import java.io.Writer;

/**
 *
 * @author dmsibley
 */
public class TimeFlushingWriter extends Writer {
	protected final Writer wrapped;
	protected long lastFlushed;
	protected int checkAgain;
	protected final int minWrites;

	public TimeFlushingWriter(Writer out) {
		this(out, 500);
	}

	public TimeFlushingWriter(Writer out, int minWrites) {
		this.wrapped = out;
		this.lastFlushed = 0;
		this.checkAgain = minWrites - 20; // feed it a little bit, so the first write happens soon, but not off the bat.
		this.minWrites = minWrites;
	}

	@Override
	public void write(char[] cbuf, int off, int len) throws IOException {
		wrapped.write(cbuf, off, len);
		checkAgain = checkAgain + 1;
		if (checkAgain > minWrites &&
				(System.currentTimeMillis() - lastFlushed) > 15000) {
			this.flush();
		}
	}

	@Override
	public void flush() throws IOException {
		wrapped.flush();
		lastFlushed = System.currentTimeMillis();
		checkAgain = 0;
	}

	@Override
	public void close() throws IOException {
		wrapped.close();
	}

}
