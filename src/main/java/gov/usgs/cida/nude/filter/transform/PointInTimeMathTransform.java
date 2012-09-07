package gov.usgs.cida.nude.filter.transform;

import gov.usgs.cida.nude.column.Column;
import gov.usgs.cida.nude.filter.ColumnTransform;
import gov.usgs.cida.nude.resultset.inmemory.TableRow;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author dmsibley
 */
public class PointInTimeMathTransform implements ColumnTransform {
	private static final Logger log = LoggerFactory.getLogger(PointInTimeMathTransform.class);
	
	protected final Column col;
	protected final Column timeCol;
	protected final MathFunction func;
	protected final List<BigDecimal> funcParams;

	public PointInTimeMathTransform(Column timeCol, Column col, MathFunction func, List<BigDecimal> funcParams) {
		this.col = col;
		this.timeCol = timeCol;
		this.func = func;
		this.funcParams = funcParams;
	}
	
	@Override
	public String transform(TableRow row) {
		String result = null;
		String val = row.getValue(col);
		String time = row.getValue(timeCol);
		
		List<BigDecimal> toRun = new ArrayList<BigDecimal>();
		if (StringUtils.isEmpty(time)) {
			toRun.add(null);
		} else {
			toRun.add(new BigDecimal(time));
		}
		
		if (StringUtils.isNotEmpty(val)) {
			toRun.add(new BigDecimal(val));
		}
		
		toRun.addAll(funcParams);
		
		BigDecimal resultBD = func.run(toRun);
		if (null != resultBD) {
			result = resultBD.toPlainString();
		}
		
		return result;
	}
	
}
