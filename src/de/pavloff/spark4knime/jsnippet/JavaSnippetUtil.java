/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   16.03.2012 (hofer): created
 */
package de.pavloff.spark4knime.jsnippet;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.FileUtil;

/**
 * Utility method for the Java Snippet nodes.
 *
 * @author Heiko Hofer
 */
public final class JavaSnippetUtil {

    private JavaSnippetUtil() {
        // private constructor to prevent instantiation.
    }

    /** Convert file location to File. Also accepts file in URL format
     * (e.g. local drop files as URL).
     * @param location The location string.
     * @return The file to the location
     * @throws InvalidSettingsException if argument is null, empty or the file
     * does not exist.
     */
    public static final File toFile(final String location) throws InvalidSettingsException {
        CheckUtils.checkSetting(StringUtils.isNotEmpty(location), "Invalid (empty) jar file location");
        File result;
        URL url = null;
        try {
            url = new URL(location);
        } catch (MalformedURLException mue) {
        }
        if (url != null) {
            result = FileUtil.getFileFromURL(url);
        } else {
            result = new File(location);
        }
        CheckUtils.checkSetting(result != null && result.exists(),
                "Can't read file \"%s\"; invalid class path", location);
        return result;
    }

}
