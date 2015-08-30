package de.pavloff.spark4knime.input;

import org.knime.core.node.NodeView;

import de.pavloff.spark4knime.TableCellUtils.RddViewer;

/**
 * <code>NodeView</code> for the "TableToRDD" Node. Read a table from previous
 * node as input and parallelize data by rows.
 * 
 * Viewer is handled by NodeModel. Only during onOpen() the data will be taken
 * from RDD and showed.
 * 
 * @author Oleg Pavlov, University of Heidelberg
 */
public class TableToRDDNodeView extends NodeView<TableToRDDNodeModel> {

	/**
	 * Creates a new view.
	 * 
	 * @param nodeModel
	 *            The model (class: {@link TableToRDDNodeModel})
	 */
	protected TableToRDDNodeView(final TableToRDDNodeModel nodeModel) {
		super(nodeModel);
		// instantiate the components of the view here.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void modelChanged() {
		// retrieve the new model from your nodemodel and update the view.
		TableToRDDNodeModel nodeModel = (TableToRDDNodeModel) getNodeModel();
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
		TableToRDDNodeModel nodeModel = (TableToRDDNodeModel) getNodeModel();
		assert nodeModel != null;
		RddViewer view = nodeModel.getRddViewer();
		assert (view != null);

		setComponent(view.getTableView());
	}

}
