package io.teknek.farsandra.maven.plugin.goals;

import io.teknek.farsandra.Farsandra;
import io.teknek.farsandra.maven.plugin.Constants;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;

/**
 * Stop goal for Farsandra Maven Plugin.
 *
 * @author Andrea Gazzarini
 * @since 1.0
 */
@Mojo(name="stop")
public final class Stop extends AbstractMojo {
    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        final Farsandra farsandra = (Farsandra) getPluginContext().remove(Constants.FARSANDRA);
        if (farsandra != null && farsandra.getManager().isRunning()) {
            farsandra.getManager().destroy();
        }
    }
}
