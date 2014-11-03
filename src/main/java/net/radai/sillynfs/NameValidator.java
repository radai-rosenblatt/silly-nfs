package net.radai.sillynfs;

import org.dcache.nfs.status.AccessException;
import org.dcache.nfs.status.NameTooLongException;

/**
 * Created on 13/10/2014.
 */
public interface NameValidator {
    void checkName(String name) throws NameTooLongException, AccessException;

    public static final NameValidator LOOSE = new NameValidator() {
        @Override
        public void checkName(String name) throws NameTooLongException, AccessException {
            if (name==null || name.isEmpty() || name.equals(".")) {
                throw new AccessException();
            }
        }
    };
}
