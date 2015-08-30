package de.pavloff.spark4knime.output;

import org.knime.core.node.NodeView;

/**
 * <code>NodeView</code> for the "RDDToTextFile" Node. Save elements of the RDD
 * as text file.
 * 
 * The node has no view.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class RDDToTextFileNodeView extends NodeView<RDDToTextFileNodeModel> {

	/**
	 * Creates a new view.
	 * 
	 * @param nodeModel
	 *            The model (class: {@link RDDToTextFileNodeModel})
	 */
	protected RDDToTextFileNodeView(final RDDToTextFileNodeModel nodeModel) {
		super(nodeModel);
		// instantiate the components of the view here.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void modelChanged() {
		// retrieve the new model from your nodemodel and update the view.
		RDDToTextFileNodeModel nodeModel = (RDDToTextFileNodeModel) getNodeModel();
		assert nodeModel != null;

		// be aware of a possibly not executed nodeModel! The data you retrieve
		// from your nodemodel could be null, emtpy, or invalid in any kind.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onClose() {
		// things to do when closing the view
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void onOpen() {
		// things to do when opening the view
	}

}
