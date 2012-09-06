package gov.usgs.cida.nude.filter.transform;

import java.math.BigDecimal;

/**
 *
 * @author dmsibley
 */
public interface MathFunction {
	public BigDecimal run(Iterable<BigDecimal> items);
}
