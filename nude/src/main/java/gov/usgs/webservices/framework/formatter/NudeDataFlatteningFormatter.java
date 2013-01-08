/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package gov.usgs.webservices.framework.formatter;

import gov.usgs.webservices.framework.basic.MimeType;
import static gov.usgs.webservices.framework.basic.MimeType.*;
import gov.usgs.webservices.framework.basic.PipelineComponent;
import gov.usgs.webservices.framework.utils.URIUtils;
import gov.usgs.webservices.framework.utils.XMLUtils;
import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

/**
 * CopyPasta Patch for not escaping problematic values in the header.
 * @author dmsibley
 */
/**
 * DataFlatteningFormatter accepts an arbitrary XML Stream and renders it as
 * a Resultset-like HTML table. Basically, it treats children of the root
 * elements as rows, and each of the attributes and children as columns.
 *
 * The Formatter can be configured to recognize either elements at a certain
 * depth as rows, or recognize specified elements as rows. Use the methods
 * setRowElementName() for this. By default, the
 * depth level is set to 3, e.g. the grandchildren elements of the root element.
 *
 * CAVEAT: Assumes the first element found is complete.
 *
 * @author ilinkuo
 *
 */
public class NudeDataFlatteningFormatter extends AbstractFormatter implements IFormatter {

	private static final String DEFAULT_AUTHOR = "USGS";
	// CONSTANTS
	public static final int DEFAULT_ROW_DEPTH_LEVEL = 3;
	public static final MimeType DEFAULT_MIMETYPE = HTML;
	public static final Set<MimeType> acceptableTypes = defineAcceptableTypes();
	public static final String FLATTENING_PARAM = "needsCompleteFirstRow";

	private static final String EMPTY_STRING = "";
	private static final String NONSENSE_ROW_ELEMENT_IDENTIFIER = "!";

	// ======================
	// PUBLIC UTILITY METHODS
	// ======================
	protected static Set<MimeType> defineAcceptableTypes(){
		return EnumSet.of(EXCEL, HTML, XHTML, XML, CSV, TAB);
	}

	/**
	 * Returns true if the DataFlatteningFormatter class ( not the instance ) may accept the output type
	 * @param type
	 * @return
	 */
	public static boolean mayAccept(MimeType type) {
		return acceptableTypes.contains(type);
	}

	// ====================
	// CONFIGURATION FIELDS
	// ====================
	public boolean isSilent; // set true to not output error messages
	protected int depthLevel;
	protected String author = DEFAULT_AUTHOR;
	protected int ROW_DEPTH_LEVEL = DEFAULT_ROW_DEPTH_LEVEL;
	protected String ROW_ELEMENT_IDENTIFIER = NONSENSE_ROW_ELEMENT_IDENTIFIER; // nonsense value by default so nothing can be matched
	protected final Delimiters delims;
	protected boolean isKeepElders; // true if elder element information is used to determine nestings
	protected boolean isDoCopyDown = true;
	protected boolean ignoreRowElement = false; // false for backward compatibility
	protected Set<String> ignoredAttributes = new HashSet<String>();
	protected Map<String, String> contentDefinedElements = new HashMap<String, String>();
	protected boolean throwExceptionOnTooManyValues = false;

	// ============
	// CONSTRUCTORS
	// ============
	public NudeDataFlatteningFormatter() {
		this(DEFAULT_MIMETYPE, false);
	}

	public NudeDataFlatteningFormatter(MimeType type, boolean throwExceptions) { //, boolean throwExceptions
		super(type);
		this.throwExceptionOnTooManyValues = throwExceptions;
		
		switch (type) {
		case EXCEL:
			// TODO use configuration parameter
			delims = Delimiters.makeExcelDelimiter(author, new Date().toString());
			break;
		case HTML:
		case XHTML:
		case XML:
			delims = Delimiters.HTML_DELIMITERS;
			break;
		case CSV:
			delims = Delimiters.CSV_DELIMITERS;
			break;
		case TAB:
			delims = Delimiters.TAB_DELIMITERS;
			break;
		default:
			if (acceptableOutputTypes.contains(type)) {
				throw new UnsupportedOperationException(type + " is accepted but not yet implemented");
			}
			throw new IllegalArgumentException(type + " not accepted by " + gov.usgs.webservices.framework.formatter.DataFlatteningFormatter.class.getSimpleName());
		}
		acceptableOutputTypes = EnumSet.of(type);
		ignoreAttribute("schemaLocation");
		ignoreAttribute("encodingStyle");
		ignoreAttribute("xsi:schemaLocation");
		ignoreAttribute("xmlns:xsi");
		ignoreAttribute("xmlns");
	}

	// ===============
	// UTILITY METHODS
	// ===============
	/**
	 * isCompleteFirstRow() tests whether a String value is due to a complete
	 * first row directive. This takes advantage of the Oracle quirk of
	 * returning a null instead of an empty string. It is assumed that empty
	 * strings are only created by an artificial first row resultset.
	 *
	 * @param value
	 * @return true if this value is due to a complete first row directive.
	 */
	public static boolean isCompleteFirstRow(String value) {
		return value != null && value.length() == 0;
	}


	// ====================================
	// PRIVATE CLASSES: Element, ParseState
	// ====================================
	private static class Element{
		public final String fullName;
		public String displayName;
		public final String localName;
		public boolean hasChildren;

		public Element(String full, String local, String displayName) {
			this.fullName = full;
			this.localName = local;
			this.displayName = (displayName == null)? local: displayName;
		}

		@Override
		public int hashCode() {
			return fullName.hashCode();
		};

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !(obj instanceof NudeDataFlatteningFormatter.Element)) {
				return false;
			}
			NudeDataFlatteningFormatter.Element that = (NudeDataFlatteningFormatter.Element) obj;
			return (fullName.equals(that.fullName));
		}

		public void addParentToDisplayName() {
			this.displayName = URIUtils.parseQualifiedName(this.fullName, this.displayName);
		}

	}

	private class ParseState{
		// global state fields
		boolean hasEncounteredTargetContent;
		int row; // number of target row elements encountered

		// row state fields
		boolean isInTarget;
		boolean isProcessingHeaders;

		// context tracks the parsing depth within the document
		public Stack<String> context = new Stack<String>();
		public String targetElementContext = EMPTY_STRING;
		/**
		 * Tracks the currently open target element until a child of that element is encountered
		 */
		NudeDataFlatteningFormatter.Element currentTargetColumn;
		/**
		 * Tracks the currently open elder element until a child of that element
		 * is encountered. An elder is an ancestor, uncle, or preceding sibling of a
		 * target element.
		 */
		NudeDataFlatteningFormatter.Element currentElder;

		// output fields
		public Set<NudeDataFlatteningFormatter.Element> targetColumnList = new LinkedHashSet<NudeDataFlatteningFormatter.Element>();
		public Set<NudeDataFlatteningFormatter.Element> elderColumnList = new LinkedHashSet<NudeDataFlatteningFormatter.Element>();
		public Map<String, String> targetColumnValues = new HashMap<String, String>();
		public Map<String, String> elderColumnValues = new HashMap<String, String>();

		// --------------------
		// STATE UPDATE METHODS
		// --------------------
		/**
		 * Updates the state at the beginning of a StAX StartElement event. In
		 * addition, returns the display name of the element if it is not the
		 * same as the local name.
		 *
		 * @param localName
		 */
		public String startElementBeginUpdate( XMLStreamReader in) {
			String localName = in.getLocalName();
			String displayName = localName;
			if (isCurrentElementContentDefined(in)) {
				displayName = localName + "-" + in.getAttributeValue(null, contentDefinedElements.get(localName))
				.replace(URIUtils.SEPARATOR, '-');
			}
			// Bookkeeping: the top of the context stack always points to the current element
			String contextName = makeFullName(context.peek(), displayName);
			context.push(contextName);
			if (isOnTargetRowStartOrEnd(localName)) {
				// "Correct" the context because we want to use shorter contexts
				// while within the target. This makes the within target context
				// a *relative* one, which allows us to properly handle the target
				// if it occurs at different places or different levels within
				// the document. Using the absolute context fails to handle
				// uneven hierarchies correctly.
				context.pop();
				context.push(localName);

				// Nonetheless, we want to remember the target element. In this
				// case, we use the full context rather than the abbreviated
				// one.
				targetElementContext = contextName;
				row++;

				// Reset the row parsing status
				isInTarget = true;
				isProcessingHeaders = (row == 1);
				targetColumnValues.clear();
			}
			// only return displayName if it's actually different
			return (displayName.equals(localName))? null: displayName;
		}

		/**
		 * Updates the state at the end of a StAX EndElement event
		 * @param onTargetEnd
		 */
		public void finishEndElement(boolean onTargetEnd) {
			String current = context.pop();
			boolean isElderElement = !isInTarget;
			if (isElderElement && isKeepElders) {
				if (isDoCopyDown) {
					clearAncestralDescendants(current);
				}
				// now backtracking. currentElder only tracks going down.
				currentElder = null;
			} else { // not a elder, in the target "row"
				currentTargetColumn = null;
			}
			if (onTargetEnd) {
				isInTarget = false; // exiting target
			}
		}

		// ----------------
		// STATE INDICATORS
		// ----------------
		public boolean isOnTargetRowStartOrEnd(String localName) {
			return context.size() == ROW_DEPTH_LEVEL || ROW_ELEMENT_IDENTIFIER.equals(localName);
		}

		/**
		 * Returns true if at least one target has been found. The difference
		 * between isTargetFound() and hasEncounteredContent is that the the
		 * first is true immediately upon encountering the start tag and that
		 * the latter becomes true only when nonempty tag content or attribute
		 * content within the target is encountered.
		 *
		 * @return
		 */
		public boolean isTargetFound() {
			return (row > 0);
		}

		// ---------------
		// SERVICE METHODS
		// ---------------
		/**
		 * Convenience method for creating an element.
		 * @param localName
		 * @param displayName
		 * @return
		 */
		public NudeDataFlatteningFormatter.Element makeElement(String localName, String displayName) {
			return new NudeDataFlatteningFormatter.Element(context.peek(), localName, displayName);
		}

		/**
		 * Adds a target header or column. Should only be called within a target
		 * @param localName
		 * @param displayName
		 */
		public void addHeaderOrColumn(String localName, String displayName) {
			if (isInTarget) {
				NudeDataFlatteningFormatter.Element e = makeElement(localName, displayName);
				boolean isNew = targetColumnList.add(e);
				// Note that the previous column element has child tags so shouldn't
				// be output as data flattening isn't set to deal with document
				// style xml.
				if (isNew) {
					if (currentTargetColumn != null) {
						currentTargetColumn.hasChildren = true;
					}
					if (currentElder != null) {
						currentElder.hasChildren = true;
					}
					currentTargetColumn = e; // update

					// special case
					if (ignoreRowElement && localName.equals(ROW_ELEMENT_IDENTIFIER)) {
						e.hasChildren = true;
					}
				}
			}
		}

		/**
		 * Adds an elder target header or column. Should only be called before a
		 * target is ever found. We can't deal with nontarget columns whose
		 * first appearance is after a target.
		 *
		 * @param localName
		 */
		public void addElderHeaderOrColumn(String localName) {
			if (!isTargetFound()) {
				if (isKeepElders) {
					// TODO need to see if this needs a display name, and test
					NudeDataFlatteningFormatter.Element e = makeElement(localName, null);
					boolean isNew = elderColumnList.add(e);
					// Note that the previous column element has child tags so shouldn't
					// be output as data flattening isn't set to deal with document
					// style xml.
					if (isNew) {
						if (currentElder != null) {
							currentElder.hasChildren = true;
						}
						currentElder = e; // update
					}
				}
			}
		}

		public boolean hasTargetContent() {
			if (hasEncounteredTargetContent) {
				return true;
			}
			for (String value: targetColumnValues.values()) {
				if (value != null && value.length() > 0) {
					hasEncounteredTargetContent = true;
					return true;
				}
			}
			return false;
		}

		/**
		 * Clears all descendant values of an ancestor of the target, excluding
		 * the target itself.
		 *
		 * @param fullName
		 */
		public void clearAncestralDescendants(String fullName) {
			if (!URIUtils.isAncestorOf(fullName, targetElementContext)) {
				// it's not a direct ancestor of the target, so don't do anything
				return;
			}
			boolean isAncestorFound = false;
			for (NudeDataFlatteningFormatter.Element elderColumn: elderColumnList) {
				String elementName = elderColumn.fullName;
				// Make use of the fact that elderColumnList is an ordered set
				// ordered by document order to know that once you've found an
				// ancestor, you can delete everything after it.
				if (isAncestorFound || URIUtils.isAncestorOf(fullName, elementName)) {
					isAncestorFound = true;
					elderColumnValues.remove(elementName);
				}
			}
		}

		/**
		 * Store the character text using the current element name as key
		 * @param value
		 */
		public void putChars(String value) {
			if (isInTarget) {
				targetColumnValues.put(context.peek(), value);
			} else if (isKeepElders) {
				elderColumnValues.put(context.peek(), value);
			}
		}

	}

	private class ErrorChecker{
		int ehCount = 0, thCount = 0, evCount = 0, tvCount=0;
		private boolean throwExceptions;

		public ErrorChecker(boolean throwExceptions) {
			this.throwExceptions = throwExceptions;
		}
		
		// update methods
		public void updateElderHeaderCount(int size) {
			ehCount = Math.max(ehCount, size);			
		}
		public void updateTargetHeaderCount(int size) {
			thCount = Math.max(thCount, size);		
		}
		public void updateElderValueCount(int size) {
			evCount = Math.max(evCount, size);		
		}
		public void updateTargetValueCount(int size) {
			tvCount = Math.max(tvCount, size);		
		}
		
		// checks
		public boolean hasTooManyElderValuesError() {
			if (throwExceptions && evCount > ehCount) {
				throw new RuntimeException("Too many elder values -- headers: " + ehCount + " < values: " + evCount);
			}
			return evCount > ehCount;
		}
		public boolean hasTooManyTargetValuesError() {
			if (throwExceptions && tvCount > thCount) {
				throw new RuntimeException("Too many elder values -- headers: " + thCount + " < values: " + tvCount);
			}
			return tvCount > thCount;
		}
	}
	
	// =====================
	// CONFIGURATION METHODS
	// =====================
	/**
	 *
	 * @param level
	 * @return 
	 */
	public NudeDataFlatteningFormatter setDepthLevel(int level) {
		if (level <= 0) {
			throw new IllegalArgumentException("level must be > 0");
		}
		depthLevel = level;
		ROW_DEPTH_LEVEL = depthLevel + 1;
		ROW_ELEMENT_IDENTIFIER = "!"; //set the ROW_ELEMENT_NAME to an illegal name so that it never triggers
		return this;
	}

	public NudeDataFlatteningFormatter setRowElementName(String name) {
		if (depthLevel > 0) {
			throw new IllegalStateException("Can only set depthLevel or rowElementName, not both");
		}
		ROW_DEPTH_LEVEL = 1000; // set the ROW_DEPTH_LEVEL so deep that it never triggers
		ROW_ELEMENT_IDENTIFIER = name;
		return this;
	}

	/**
	 * Set true to keep the information from elder elements when flattening
	 * @param isKeepElder
	 * @return 
	 */
	public NudeDataFlatteningFormatter setKeepElderInfo(boolean isKeepElder) {
		this.isKeepElders = isKeepElder;
		return this;
	}

	/**
	 * Set true to copy down all of elders information when flattening
	 * @param copyDown
	 * @return 
	 */
	public NudeDataFlatteningFormatter setCopyDown(boolean copyDown) {
		// You can't copy down elders info without keeping it, can you?
		this.isDoCopyDown = copyDown;
		if (copyDown) {
			this.isKeepElders = true;
		}
		return this;

	}

	public NudeDataFlatteningFormatter setIgnoreRowElement(boolean ignore) {
		this.ignoreRowElement = ignore;
		return this;
	}

	public NudeDataFlatteningFormatter ignoreAttribute(String attributeName) {
		ignoredAttributes.add(attributeName);
		return this;
	}

	public NudeDataFlatteningFormatter addContentDefinedElement(String elementName, String attributeName) {
		if (elementName != null ) {
			contentDefinedElements.put(elementName, attributeName);
		}
		return this;
	}

	// ==============
	// SERVICE METHOD
	// ==============
	/**
	 * @see gov.usgs.webservices.framework.formatter.AbstractFormatter#dispatch(javax.xml.stream.XMLStreamReader, java.io.Writer)
	 *
	 * Note that namespaces are ignored.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void dispatch(XMLStreamReader in, Writer out) throws IOException {
		if (in instanceof PipelineComponent) {
			PipelineComponent pc = (PipelineComponent) in;
			if (pc.isFlattenable()) {
				boolean isComplete = pc.setOutputCompleteFirstRow(true);
				// TODO check status of isComplete
				if (NONSENSE_ROW_ELEMENT_IDENTIFIER == ROW_ELEMENT_IDENTIFIER) {
					// [IK] deliberately used == comparison rather than equals()
					String rowElementName = pc.getRowElementName();
					if (rowElementName != null) {
						ROW_ELEMENT_IDENTIFIER = rowElementName;
					}
					this.ignoreRowElement = pc.shouldIgnoreRowElement();
				}
			}
		}

		NudeDataFlatteningFormatter.ParseState state = new NudeDataFlatteningFormatter.ParseState();
		NudeDataFlatteningFormatter.ErrorChecker ec = new NudeDataFlatteningFormatter.ErrorChecker(throwExceptionOnTooManyValues);

		try {
			// initialize the context stack to avoid empty stack errors
			state.context.push("");

			out.write(delims.sheetStart);
			boolean done = false;
			while (!done && in.hasNext()) {
				int event = in.next();
				switch (event) {
				case XMLStreamConstants.START_DOCUMENT:
					break; // no start document handling needed
				case XMLStreamConstants.START_ELEMENT:

					String localName = in.getLocalName();

					boolean isContentDefined = isCurrentElementContentDefined(in);
					String contentAttributeName = (isContentDefined)? contentDefinedElements.get(localName): null;
					String displayName = state.startElementBeginUpdate(in);

					if (state.isTargetFound() && state.isInTarget){
						// PROCESS THE ELEMENT HEADERS
						// Read and record the column headers from the first row's elements.
						// Add columns for later rows, but they don't get headers because
						// we're streaming and can't go back to the column headers.
						state.addHeaderOrColumn(localName, displayName);

						// PROCESS/STORE ATTRIBUTE HEADERS AND NAME/VALUES
						int attCount = in.getAttributeCount();
						for (int i=0; i< attCount; i++) {
							String attLocalName = in.getAttributeLocalName(i);
							if (!ignoredAttributes.contains(attLocalName) && !(isContentDefined && attLocalName.equals(contentAttributeName))) {
								// ignore the schemaLocation attribute
								String fullName = makeFullName(state.context.peek(), attLocalName);
								NudeDataFlatteningFormatter.Element att = new NudeDataFlatteningFormatter.Element(fullName, attLocalName, null);
								state.targetColumnList.add(att);
								state.targetColumnValues.put(fullName, in.getAttributeValue(i).trim());
							}
						}
					} else if (isKeepElders && !state.isInTarget) {
						state.addElderHeaderOrColumn(localName);

						// PROCESS/STORE ATTRIBUTE HEADERS AND NAME/VALUES
						int attCount = in.getAttributeCount();
						for (int i=0; i< attCount; i++) {
							String attLocalName = in.getAttributeLocalName(i);
							if (!ignoredAttributes.contains(attLocalName) && !(isContentDefined && attLocalName.equals(contentAttributeName))) {
								String fullName = makeFullName(state.context.peek(), attLocalName);
								NudeDataFlatteningFormatter.Element att = new NudeDataFlatteningFormatter.Element(fullName, attLocalName, null);
								state.elderColumnList.add(att);
								state.elderColumnValues.put(fullName, in.getAttributeValue(i).trim());
							}
						}
					}

					break;
				case XMLStreamConstants.CHARACTERS:

					state.putChars(in.getText().trim());

					break;
					// case XMLStreamConstants.ATTRIBUTE:
					// TODO may need to handle this later
				case XMLStreamConstants.END_ELEMENT:
					localName = in.getLocalName();

					// Write tag content
					boolean onTargetEnd = state.isOnTargetRowStartOrEnd(localName);
					if (onTargetEnd) {

						// OUTPUT HEADER row first, if not already done
						if (state.isProcessingHeaders) {
							// write out the columns headers first
							out.write(delims.headerRowStart);

							// preprocess to disambiguate common column headers
							if (isKeepElders) {
								updateQualifiedNames(state.elderColumnList, state.targetColumnList);
								//output the elder headers
								if (state.elderColumnList != null) {
									Iterator<NudeDataFlatteningFormatter.Element> iter = state.elderColumnList.iterator();
									int eHeaderCount = 0;
									while (iter.hasNext()) {
										NudeDataFlatteningFormatter.Element element = iter.next();
										if (!element.hasChildren){// don't output elements with child elements
											String cellEnd = delims.headerCellEnd; // we are making the assumption here that there are ALWAYS target columns, otherwise, it's a bit of a pain to coordinate.
											out.write(delims.headerCellStart + formatSimple(element.displayName) + cellEnd);
											eHeaderCount++;
										}
									}
									ec.updateElderHeaderCount(eHeaderCount);
								}

							} else {
								updateQualifiedNames(state.targetColumnList);
							}
							
							if (state.targetColumnList != null) {
								Iterator<NudeDataFlatteningFormatter.Element> iter = state.targetColumnList.iterator();
								int tHeaderCount = 0;
								while (iter.hasNext()) {
									NudeDataFlatteningFormatter.Element element = iter.next();
									
									if (!element.hasChildren){// don't output elements with child elements
										String cellEnd = (iter.hasNext())? delims.headerCellEnd : delims.lastHeaderCellEnd;
										out.write(delims.headerCellStart + formatSimple(element.displayName) + cellEnd);
										tHeaderCount++;
									}
								}
								ec.updateTargetHeaderCount(tHeaderCount);
							}

							out.write(delims.headerRowEnd);
							// bookkeeping
							state.isProcessingHeaders = false;
						}

						// OUTPUT DATA row only if there is content
						if (state.hasTargetContent()) {
							out.write(delims.bodyRowStart);
							if (isKeepElders) {
								if (state.elderColumnList != null) {
									Iterator<NudeDataFlatteningFormatter.Element> iter = state.elderColumnList.iterator();
									int eValueCount = 0;
									while (iter.hasNext()) {
										NudeDataFlatteningFormatter.Element element = iter.next();
										if (!element.hasChildren){
											// don't output elements with child elements
											String value = state.elderColumnValues.get(element.fullName);
											value = (value != null)? value: "";
											String cellEnd = delims.bodyCellEnd; // as above, we assume there are always target columns
											out.write(delims.bodyCellStart + formatSimple(value) +  cellEnd);
											eValueCount++;
										}
									}
									ec.updateElderValueCount(eValueCount);
								}

								if (!isDoCopyDown) {
									// clear ALL the elder values
									state.elderColumnValues.clear();
								}
							}

							if (state.targetColumnList != null) {
								Iterator<NudeDataFlatteningFormatter.Element> iter = state.targetColumnList.iterator();
								int tValueCount = 0;
								while (iter.hasNext()) {
									NudeDataFlatteningFormatter.Element element = iter.next();
									if (!element.hasChildren){
										// don't output elements with child elements
										String value = state.targetColumnValues.get(element.fullName);
										value = (value != null)? value: "";
										String cellEnd = (iter.hasNext())? delims.bodyCellEnd: delims.lastBodyCellEnd;
										out.write(delims.bodyCellStart + formatSimple(value) +  cellEnd);
										tValueCount++;
									}
								}
								ec.updateTargetValueCount(tValueCount);
							}

							out.write(delims.bodyRowEnd);
						}
					}
					state.finishEndElement(onTargetEnd);
					break;
				case XMLStreamConstants.END_DOCUMENT:
					done = true;
					break;
				}
			}
		} catch (XMLStreamException e) {
			e.printStackTrace();
		} catch (EmptyStackException e) {
			e.printStackTrace();
		} finally {
			out.write(delims.sheetEnd);
			if (!isSilent && ec.hasTooManyElderValuesError()) System.err.println("DataFlattener: Number of elder values exceeds number of headers");
			if (!isSilent && ec.hasTooManyTargetValuesError()) System.err.println("DataFlattener: Number of target values exceeds number of headers");
		}
		out.flush();

	}

	private void updateQualifiedNames(Set<NudeDataFlatteningFormatter.Element>... columnLists) {




		boolean hasDuplicates = true;
		while (hasDuplicates) { //for (int i = 0; hasDuplicates && i < 10; i++) { //Use this way to make sure we don't infinite loop?
			hasDuplicates = false;
			Set<String> allDisplayNames = new HashSet<String>();
			Set<String> duplicates = new HashSet<String>();
			// First create a collection of the duplicates. We only care about the
			// childless ones, however.
			for (Set<NudeDataFlatteningFormatter.Element> columnList: columnLists) {
				for (NudeDataFlatteningFormatter.Element element: columnList) {
					if (!element.hasChildren) {
						boolean isUnique = allDisplayNames.add(element.displayName);
						if (!isUnique) {
							duplicates.add(element.displayName);
							hasDuplicates = true;
						}
					}
				}
			}

			// now go through and update the qualified name
			for (Set<NudeDataFlatteningFormatter.Element> columnList: columnLists) {
				for (NudeDataFlatteningFormatter.Element element: columnList) {
					if (!element.hasChildren) {
						if (duplicates.contains(element.displayName)) {
							element.addParentToDisplayName(); //displayName = URIUtils.parseQualifiedName(element.fullName, element.displayName);
						}
					}
				}
			}
		}
	}

	// ===============
	// UTILITY METHODS
	// ===============
	private boolean isCurrentElementContentDefined(XMLStreamReader in) {
		String localName = in.getLocalName();
		String contentAttribute = contentDefinedElements.get(localName);
		if (contentAttribute != null) {
			// This is a content defined element only if it
			// matches the local name and has a corresponding attribute value;
			return in.getAttributeValue(null, contentAttribute) != null;
		}
		return false;
	}

	private static final Pattern quoteLiteral = Pattern.compile("\"");
	private String formatSimple(String value) {
		if (value == null) {
			return "";
		}
		switch (this.outputType) {
		case TAB:
			value = XMLUtils.unEscapeXMLEntities(value);
			value = value.replaceAll("[\n\r]", EMPTY_STRING); //Handles newlines and carriage returns, 
			// so we don't break lines in the middle of a row
			return value;
		case CSV:
			// Currently handles commas and quotes. May need to handle carriage
			// returns and tabs later?
			value = XMLUtils.unEscapeXMLEntities(value);
			boolean hasQuotes = value.indexOf('"') >= 0;
			boolean isDoEncloseInQuotes = (value.indexOf(',')>=0) || hasQuotes;
			if (hasQuotes) {
				Matcher matcher = quoteLiteral.matcher(value);
				value = matcher.replaceAll("\"\""); // escape quotes by doubling them
			}
			return (isDoEncloseInQuotes)?  '"' + value + '"': value;
		case XML: // same as excel
		case EXCEL:
			return XMLUtils.quickTagContentEscape(value);
		}
		return value; // by default
	}

	private String makeFullName(String context, String name) {
		return (context.length() > 0)? context + URIUtils.SEPARATOR + name: name;
	}

	@Override
	public boolean isNeedsCompleteFirstRow() {
		return true;
	}

}